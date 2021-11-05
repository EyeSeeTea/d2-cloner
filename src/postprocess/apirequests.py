import datetime
import json
from datetime import time

import requests

from d2apy import dhis2api

from src.postprocess.list_modifier import *
from src.common.debug import debug


def init_api(url, username, password):
    return dhis2api.Dhis2Api(url, username, password)


def wait_for_server(api, timeout=3900):
    "Sleep until server is ready to accept requests"
    debug("Check active API: %s" % api.api_url)
    import time as time_
    start_time = time_.time()
    while True:
        try:
            api.get("/me")
            break
        except requests.exceptions.HTTPError:
            if time.time() - start_time > timeout:
                raise RuntimeError("Timeout: could not connect to the API")
            time.sleep(3000)
        except requests.exceptions.ConnectionError:
            if time.time() - start_time > timeout:
                raise RuntimeError("Timeout: could not connect to the API")
            time_.sleep(10)


def activate(api, users):
    debug("Activating %d user(s)..." % len(users))
    for user in users:
        user["userCredentials"]["disabled"] = False
        api.put("/users/" + user["id"], user)


def delete_others(api, users):
    "Delete all the users except the given ones and the one used by the api"
    usernames = [get_username(x) for x in users] + [api.username]
    users_to_delete = api.get(
        "/users",
        {
            "paging": False,
            "filter": "userCredentials.username:!in:[%s]" % ",".join(usernames),
            "fields": "id,userCredentials[username]",
        },
    )["users"]
    # Alternatively, we could get all the users with 'fields':
    # 'id,userCredentials[username]' and loop only over the ones whose
    # username is not in usernames.
    debug("Deleting %d users..." % len(users_to_delete))
    users_deleted = []
    users_with_error = []
    for user in users_to_delete:
        try:
            api.delete("/users/" + user["id"])
            users_deleted.append(get_username(user))
        except requests.exceptions.HTTPError:
            users_with_error.append(get_username(user))
    if users_with_error:
        debug("Could not delete %d users: %s" % (len(users_with_error), users_with_error))


def add_roles(api, users, roles_to_add):
    debug("Adding %d roles to %d users..." % (len(roles_to_add), len(users)))
    for user in users:
        roles = unique(get_roles(user) + roles_to_add)
        user["userCredentials"]["userRoles"] = roles
        api.put("/users/" + user["id"], user)


def remove_groups(api, users, groups_to_remove_from):
    debug("Removing %d users from %d groups..." % (len(users), len(groups_to_remove_from)))
    response = api.get(
        "/userGroups",
        {
            "paging": False,
            "filter": "name:in:[%s]" % ",".join(groups_to_remove_from),
            "fields": ("*"),
        },
    )
    for group in response["userGroups"]:
        group["users"] = [
            user
            for user in group["users"]
            if user not in map(lambda element: pick(element, ["id"]), users)
        ]
        api.put("/userGroups/" + group["id"], group)


def get_users_by_usernames(api, usernames):
    "Return list of users corresponding to the given usernames"
    debug("Get users: usernames=%s" % usernames)

    if not usernames:
        return []

    response = api.get(
        "/users",
        {
            "paging": False,
            "filter": "userCredentials.username:in:[%s]" % ",".join(usernames),
            "fields": ":all,userCredentials[:all,userRoles[id,name]]",
        },
    )
    return response["users"]


def get_users_by_group_names(api, user_group_names):
    "Return list of users belonging to any of the given user groups"
    debug("Get users from groups: names=%s" % user_group_names)

    if not user_group_names:
        return []

    response = api.get(
        "/userGroups",
        {
            "paging": False,
            "filter": "name:in:[%s]" % ",".join(user_group_names),
            "fields": ("id,name," "users[:all,userCredentials[:all,userRoles[id,name]]]"),
        },
    )
    return sum((x["users"] for x in response["userGroups"]), [])


def get_user_roles_by_name(api, user_role_names):
    "Return list of roles corresponding to the given role names"
    debug("Get user roles: name=%s" % user_role_names)
    response = api.get(
        "/userRoles",
        {"paging": False, "filter": "name:in:[%s]" % ",".join(user_role_names), "fields": ":all"},
    )
    return response["userRoles"]


def import_json(
    api, files, importStrategy="CREATE_AND_UPDATE", mergeMode="MERGE", skipSharing="false"
):
    "Import a json file into DHIS2"
    responses = {}
    for json_file in files:
        file_to_import = json.load(open(json_file))
        response = api.post(
            "/metadata/",
            params={
                "importStrategy": importStrategy,
                "mergeMode": mergeMode,
                "skipSharing": skipSharing,
            },
            payload=file_to_import,
        )
        summary = dhis2api.ImportSummary(response["stats"])
        debug("Import Summary for %s:" % json_file)
        debug(
            "%s total: %s created - %s updated - %s deleted - %s ignored"
            % (summary.total, summary.created, summary.updated, summary.deleted, summary.ignored)
        )
        responses[json_file] = summary
    return responses


def change_server_name(api, new_name):
    debug("Changing server name to: %s" % new_name)

    if not new_name:
        debug("No new name provided - Cancelling server name change")
        return []

    response = api.post(
        "/30/systemSettings/applicationTitle", "%s" % "".join(new_name), contenttype="text/plain"
    )

    debug("change server result: %s" % response["message"])

    return response


def get_username(user):
    if "userCredentials" in user.keys():
          return user["userCredentials"]["username"]
    else:
          return user["username"]


def get_roles(user):
    return user["userCredentials"]["userRoles"]
