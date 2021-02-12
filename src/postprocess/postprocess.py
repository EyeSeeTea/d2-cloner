"""
Enable and add roles to selected users.

Example:

  import dhis2api
  import process
  api = dhis2api.Dhis2Api('http://localhost:8080/api',
                           username='admin', password='district')
  users = process.select_users(api, usernames=['test.dataentry'])
  process.add_roles(api, users, ['role1', 'role2'])
"""

import sys
import requests

from src.postprocess.postprocess import apirequests


def postprocess(cfg, entries, import_dir):
    """Execute actions on the appropriate users as specified in entries.

    The entries structure looks like:
        [
            {
                "selectUsernames": ["test.dataentry"],
                "selectFromGroups": ["program1", "program2"],
                "action": "addRoles",
                "addRoles": ["role1", "role2"]
            },
            {
                "selectFiles": ["users.json"],
                "action": "import",
                "importStrategy": "CREATE_AND_UPDATE",
                "mergeMode": "MERGE",
                "skipSharing": "false"
             }
        ]

    `action` can be "activate", "deleteOthers", "addRoles" or
    "addRolesFromTemplate", with an additional field for "addRoles" (a list)
    and "addRolesFromTemplate" (a string) if that's the action.
    """

    api = apirequests.init_api(cfg["url"], cfg["username"], cfg["password"])

    apirequests.wait_for_server(api)

    for entry in [expand_url(x) for x in entries]:
        execute(api, entry, cfg, import_dir)


def expand_url(entry):
    if not is_url(entry):
        return entry
    else:
        try:
            return requests.get(entry).json()
        except Exception as e:
            debug("Error on retrieving url with entries: %s - %s" % (entry, e))
            return {}


def is_url(x):
    return type(x) == str and x.startswith("http")


def execute(api, entry, cfg, import_dir):
    "Execute the action described in one entry of the postprocessing"
    get = lambda x: entry.get(x, [])
    contains = lambda x: x in entry

    if contains("selectUsernames") or contains("selectFromGroups"):
        users = select_users(api, get("selectUsernames"), get("selectFromGroups"))
        debug("Users selected: %s" % ", ".join(get_username(x) for x in users))
        if not users:
            return
    elif contains("selectFiles"):
        files = ["%s/%s" % (import_dir, filename) for filename in get("selectFiles")]
        debug("Files selected: %s" % ", ".join(x for x in files))
    elif contains("selectServer"):
        servers = get("selectServer")
        debug("Servers selected: %s" % ", ".join(x for x in servers))
    else:
        debug("No selection.")
        return

    action = get("action")
    if action == "activate":
        apirequests.activate(api, users)
    elif action == "deleteOthers":
        apirequests.delete_others(api, users)
    elif action == "addRoles":
        add_roles_by_name(api, users, get("addRoles"))
    elif action == "addRolesFromTemplate":
        add_roles_from_template(api, users, get("addRolesFromTemplate"))
    elif action == "import":
        apirequests.import_json(api, files)
    elif action == "changeServerName":
        apirequests.change_server_name(api, get("changeServerName"))
    elif action == "removeFromGroups":
        apirequests.remove_groups(api, users, get("removeFromGroups"))
    else:
        raise ValueError("Unknown action: %s" % action)


def select_users(api, usernames, users_from_group_names):
    "Return users with from usernames and from groups users_from_group_names"
    return unique(
        apirequests.get_users_by_usernames(api, usernames)
        + apirequests.get_users_by_group_names(api, users_from_group_names)
    )


def add_roles_by_name(api, users, rolenames):
    "Add roles to the given users"
    roles_to_add = apirequests.get_user_roles_by_name(api, rolenames)
    apirequests.add_roles(api, users, roles_to_add)


def add_roles_from_template(api, users, template_with_roles):
    "Add roles in user template_with_roles to the given users"
    template = apirequests.get_users_by_usernames(api, [template_with_roles])[0]
    roles_to_add = get_roles(template)
    apirequests.add_roles(api, users, roles_to_add)


def get_username(user):
    return user["userCredentials"]["username"]


def get_roles(user):
    return user["userCredentials"]["userRoles"]


def pick(element, properties):
    result = {}
    for property in properties:
        result[property] = element[property]
    return result


def unique(xs):
    "Return list of unique elements in xs, based on their x['id'] value"
    xs_unique = []
    seen = set()
    for x in xs:
        if x["id"] not in seen:
            seen.add(x["id"])
            xs_unique.append(x)
    return xs_unique


def debug(txt):
    print(txt)
    sys.stdout.flush()
