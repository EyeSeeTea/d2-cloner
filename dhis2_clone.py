#!/usr/bin/env python3

"""
Clone a dhis2 installation from another server.
"""
import errno
import subprocess
import sys
import os
import re
import time
import json
import argparse
from subprocess import Popen

import psycopg2
from src.preprocess import preprocess
from src.postprocess import postprocess

TIME = time.strftime("%Y-%m-%d_%H%M")
COLOR = True


def main():
    global COLOR

    args = get_args()

    if args.no_color or not os.isatty(sys.stdout.fileno()):
        COLOR = False


    cfg = get_config(args.config, args.update_config)

    pre_api_version, post_api_version = get_api_version(args, cfg)

    if args.update_config:
        update_config(args.config)
        sys.exit()

    if not args.manual_restart:
        stop_tomcat(cfg, args)

    if not args.no_backups:
        backup_db(cfg, args)
        backup_war(cfg)

    if not args.no_webapps:
        get_webapps(cfg)

    if not args.no_db:
        get_db(cfg, args)

    if args.no_preprocess:
        log("No preprocessing done, as requested.")
    elif "preprocess" in cfg:
        if cfg["pre_sql_dir"]:
            preprocess.preprocess(cfg["preprocess"], cfg["departments"], cfg["pre_sql_dir"], pre_api_version)
            add_preprocess_sql_file(args, cfg)
        else:
            log("pre_sql_dir not exist in config file")
    else:
        log("No detected preprocessing rules, skipping.")

    if args.post_sql:
        run_sql(cfg, args)

    if args.post_clone_scripts:
        execute_scripts(cfg, args)

    if not args.manual_restart:
        start_tomcat(cfg, args)
        import_dir = cfg["post_process_import_dir"] if "post_process_import_dir" in cfg else None
        if args.no_postprocess:
            log("No postprocessing done, as requested.")
        elif "api_local_url" in cfg and "postprocess" in cfg:
            postprocess.postprocess(cfg["api_local_url"], args.api_local_username, args.api_local_password, cfg["postprocess"], import_dir, post_api_version)
        else:
            log("No postprocessing done.")

        if args.post_clone_scripts:
            execute_scripts(cfg, args, is_post_tomcat=True)

    else:
        log("Server not started automatically, as requested.")
        log("No postprocessing done.")


def get_api_version(args, cfg):
    supported_versions = ["2.34", "2.36"]
    pre_api_version = None
    post_api_version = None

    if args.pre_api is not None and args.pre_api not in supported_versions:
        print("ERROR: Invalid pre api version given as param")
        sys.exit()
    if args.post_api is not None and args.post_api not in supported_versions:
        print("ERROR: Invalid post api version given as param")
        sys.exit()

    if cfg["pre_api"] not in supported_versions:
        print("ERROR: Invalid pre api version in config file")
        sys.exit()

    if cfg["post_api"] not in supported_versions:
        print("ERROR: Invalid post api version in config file")
        sys.exit()

    if args.pre_api in supported_versions:
        pre_api_version = args.pre_api
        print("Loaded " + args.pre_api + "api version for pre api calls")
    if args.post_api in supported_versions:
        post_api_version = args.post_api
        print("Loaded " + args.post_api + "api version for post api calls")

    #param version have priority to config version.
    if pre_api_version is None:
        pre_api_version = cfg["pre_api"]
    if post_api_version is None:
        post_api_version = cfg["post_api"]

    return pre_api_version, post_api_version



def add_preprocess_sql_file(args, cfg):
    if is_local_tomcat(cfg):
        args.post_sql.append(os.path.join(cfg["pre_sql_dir"], preprocess.get_file()))
    elif is_local_d2docker(cfg):
        if args.post_sql:
            preprocess.move_file(os.path.join(cfg["pre_sql_dir"], preprocess.get_file()), os.path.join(args.post_sql[0], preprocess.get_file()))
        else:
            args.post_sql.append(cfg["pre_sql_dir"])


def get_args():
    "Return arguments"
    parser = argparse.ArgumentParser(description=__doc__)
    add = parser.add_argument  # shortcut
    add("config", help="file with configuration")
    add("--db-local", help="db to be override")
    add("--db-remote", help="db to be copied in the db-local")
    add("--api-local-username", help="api local user")
    add("--api-local-password", help="api local password")
    add("--no-backups", action="store_true", help="don't make backups")
    add("--no-webapps", action="store_true", help="don't clone the webapps")
    add("--no-db", action="store_true", help="don't clone the database")
    add("--no-postprocess", action="store_true", help="don't do postprocessing")
    add("--no-preprocess", action="store_true", help="don't do preprocessing")
    add("--manual-restart", action="store_true", help="don't stop/start tomcat")
    add("--post-sql", nargs="+", default=[], help="sql files to run post-clone")
    add("--pre-api", help="Pre Api calls compatible versions: 2.34 / 2.36 (default: 2.36)")
    add("--post-api", help="Post Api calls compatible versions: 2.34 / 2.36 (default: 2.36)")
    add(
        "--post-clone-scripts",
        action="store_true",
        help="execute all py and sh scripts in post_clone_scripts_dir",
    )
    add(
        "--post-import",
        action="store_true",
        help="import to DHIS2 selected json files from post_process_import_dir",
    )
    add("--update-config", action="store_true", help="update the config file")
    add("--no-color", action="store_true", help="don't use colored output")
    add("--start-transformed", action="store_true", help="Override d2-docker image for start")
    add("--stop-transformed", action="store_true", help="Override d2-docker image for stop")
    return parser.parse_args()


def get_config(fname, update):
    "Return dict with the options read from configuration file"
    log("Reading from config file %s ..." % fname)
    try:
        with open(fname) as f:
            config = json.load(f)
        if get_version(config) != 2:
            if update:
                update_config(fname)
                sys.exit()
            else:
                raise ValueError(
                    "Old version of configuration file. Run with " "--update-config to upgrade."
                )
    except (AssertionError, IOError, ValueError) as e:
        sys.exit("Error reading config file %s: %s" % (fname, e))
    return config


def update_config(fname):
    "Update the configuration file from an old format to the current one"
    with open(fname) as f:
        config = json.load(f)

    if get_version(config) == 2:
        log("The current configuration file is valid. Nothing to update.")
    else:
        if "postprocess" in config:
            entries = []
            for entry in config["postprocess"]:
                entries += update_entry(entry)
            config["postprocess"] = entries
        name, ext = os.path.splitext(fname)
        fname_new = name + "_updated" + ext
        log("Writing updated configuration in %s" % fname_new)
        with open(fname_new, "wt") as fnew:
            json.dump(config, fnew, indent=2)


def update_entry(entry_old):
    "Return a list of dictionaries that correspond to the actions in entry"
    entries = []

    # Get the selected users part.
    users = {}
    if "usernames" in entry_old:
        users["selectUsernames"] = entry_old["usernames"]
    if "fromGroups" in entry_old:
        users["selectFromGroups"] = entry_old["fromGroups"]

    # Add a new entry for each action described in the old entry.
    old_actions = ["addRoles", "addRolesFromTemplate"]
    if all(action not in entry_old for action in old_actions):
        entry_new = users.copy()
        entry_new["action"] = "activate"
        entries.append(entry_new)
    else:
        for action in old_actions:
            if action in entry_old:
                entry_new = users.copy()
                entry_new["action"] = action
                entry_new[action] = entry_old[action]
                entries.append(entry_new)

    return entries


def get_version(config):
    "Return the highest version for which the given configuration is valid"
    if "postprocess" not in config or not config["postprocess"]:
        return 2
    for entry in config["postprocess"]:
        if "usernames" in entry or "fromGroups" in entry:
            return 1
        if "selectUsernames" in entry or "selectFromGroups" in entry:
            return 2
    raise ValueError("Unknown version of configuration file.")


def run(cmd):
    log(cmd)
    ret = os.system(cmd)
    if ret != 0:
        sys.exit(ret)


def log(txt):
    clean_txt = re.sub(r"://(.*?):(.*?)@", "://\\1:PASSWORD@", txt)
    out = "[%s] %s" % (time.strftime("%Y-%m-%d %T"), clean_txt)
    print((magenta(out) if COLOR else out))
    sys.stdout.flush()


def magenta(txt):
    return "\x1b[35m%s\x1b[0m" % txt


def execute_scripts(cfg, args, is_post_tomcat=False):
    if is_local_d2docker(cfg) and not is_post_tomcat:
        # Scripts will be executed at d2-docker start --run-scripts=DIR
        return
    dirname = cfg["post_clone_scripts_dir"]
    is_script = lambda fname: os.path.splitext(fname)[-1] in [".sh", ".py"]
    is_normal = lambda fname: not fname.startswith("post")
    is_post = lambda fname: fname.startswith("post")
    applied_filter = is_post if is_post_tomcat else is_normal
    files_list = filter(applied_filter, os.listdir(dirname))

    base_url = cfg["api_local_url"].replace("://", "://{}:{}@".format(args.api_local_username, args.api_local_password))

    for script in sorted(filter(is_script, files_list)):
        run('"%s/%s" "%s"' % (dirname, script, base_url))


def is_local_tomcat(cfg):
    server_type = cfg.get("local_type", "tomcat")
    return server_type == "tomcat"


def is_local_d2docker(cfg):
    server_type = cfg.get("local_type", "tomcat")
    return server_type == "d2-docker"


def get_local_docker_image(cfg, args, action):
    if action == "start" and args.start_transformed:
        return cfg["local_docker_image_start_transformed"]
    elif action == "stop" and args.stop_transformed:
        return cfg["local_docker_image_stop_transformed"]
    else:
        return cfg["local_docker_image"]


def start_tomcat(cfg, args):
    if is_local_tomcat(cfg):
        server_path = cfg["server_dir_local"]
        run('"%s/bin/startup.sh"' % server_path)
    elif is_local_d2docker(cfg):
        post_sql = args.post_sql[0] if args.post_sql else None
        deploy_path = cfg.get("local_docker_deploy_path", None)
        server_xml_path = cfg.get("local_docker_server_xml", None)
        dhis_conf_path = cfg.get("local_docker_dhis_conf", None)
        if post_sql and (len(args.post_sql) != 1 or not os.path.isdir(post_sql)):
            log("--post-sql for d2-docker requires a single directory")
            return
        post_scripts_dir = cfg.get("local_docker_post_clone_scripts_dir", None)
        api_url = cfg["api_local_url"]

        run(
            "d2-docker start {} --port={} --detach {} {} {} {} {}".format(
                get_local_docker_image(cfg, args, "start"),
                cfg["local_docker_port"],
                (("--deploy-path '%s'" % deploy_path) if deploy_path else ""),
                (("--tomcat-server-xml '%s'" % server_xml_path) if server_xml_path else ""),
                (("--dhis-conf '%s'" % dhis_conf_path) if dhis_conf_path else ""),
                (("--run-sql '%s'" % post_sql) if post_sql else ""),
                (("--run-scripts '%s'" % post_scripts_dir) if post_scripts_dir else ""),
                (("--auth '%s'" % (args.api_local_username + ":" + args.api_local_password)) if api_url else "")
            )
        )


def stop_tomcat(cfg, args):
    if is_local_tomcat(cfg):
        server_path = cfg["server_dir_local"]
        run('"%s/bin/shutdown.sh"' % server_path)
    elif is_local_d2docker(cfg):
        run("d2-docker stop {}".format(get_local_docker_image(cfg, args, "stop")))


def backup_db(cfg, args):
    backups_dir = cfg["backups_dir"]
    if is_local_tomcat(cfg):
        backup_name = cfg["backup_name"]
        db_local = args.db_local
        backup_file = "%s/%s_%s.dump" % (backups_dir, backup_name, TIME)
        run(
            "pg_dump --file '%s' --format custom --exclude-schema sys --clean '%s' "
            % (backup_file, db_local)
        )
    elif is_local_d2docker(cfg):
        run("d2-docker copy {} '{}'".format(get_local_docker_image(cfg, args, "stop"), backups_dir))


def backup_war(cfg):
    if is_local_tomcat(cfg):
        backups_dir = cfg["backups_dir"]
        dir_local = cfg["server_dir_local"]
        war_local = cfg["war_local"]
        backup_file = "%s/%s_%s.war" % (backups_dir, war_local[:-4], TIME)
        run('cp "%s/webapps/%s" "%s"' % (dir_local, war_local, backup_file))
    elif is_local_d2docker(cfg):
        pass


def get_webapps(cfg):
    route_local = cfg["server_dir_local"]
    route_remote = "%s:%s" % (cfg["hostname_remote"], cfg["server_dir_remote"])

    for mandatory, subdir in [[True, "webapps"], [False, "files"]]:
        cmd = "rsync -avP --delete %s/%s %s" % (route_remote, subdir, route_local)
        log(cmd)
        p = Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            universal_newlines=True,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        if p.returncode != 0 and mandatory:
            log("Mandatory folder %s failed to rsync" % subdir)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT) + "\n" + stderr, subdir)

    if is_local_tomcat(cfg):
        war_local = cfg["war_local"]
        war_remote = cfg["war_remote"]

        if war_local != war_remote:
            commands = [
                'cd "%s/webapps"' % route_local,
                'rm -f "%s"' % war_local,  # the war file itself
                'mv "%s" "%s"' % (war_remote, war_local),
                'rm -rf "%s"' % war_local[:-4],  # the directory
                'mv "%s" "%s"' % (war_remote[:-4], war_local[:-4]),
            ]
            run("; ".join(commands))


def get_db(cfg, args):
    "Replace the contents of db_local with db_remote"
    exclude = (
        "--exclude-table 'aggregated*' --exclude-table 'analytics*' "
        "--exclude-table 'completeness*' --exclude-schema sys"
    )
    dir_local = cfg["server_dir_local"]

    if is_local_tomcat(cfg):
        db_remote = args.db_remote
        dump = "pg_dump -U dhis -d '%s' --no-owner %s" % (db_remote, exclude)
        db_local = args.db_local
        empty_db(db_local)
        cmd = "ssh %s %s | psql -d '%s'" % (cfg["hostname_remote"], dump, db_local)
        run(cmd + " 2>&1 | paste - - - | uniq -c")  # run with more compact output
    elif is_local_d2docker(cfg):
        db_remote = args.db_remote
        dump = "pg_dump -U dhis -d '%s' --no-owner %s" % (db_remote, exclude)
        sql_path = os.path.join(dir_local, "db.sql.gz")
        cmd = "ssh %s %s | gzip > %s" % (cfg["hostname_remote"], dump, sql_path)
        run(cmd)
        apps_dir = os.path.join(dir_local, "files", "apps")
        documents_dir = os.path.join(dir_local, "files", "document")
        run(
            "d2-docker create data {} --sql={} {} {}".format(
                get_local_docker_image(cfg, args, "stop"),
                sql_path,
                (("--apps-dir '%s'" % apps_dir) if os.path.isdir(apps_dir) else ""),
                (("--documents-dir '%s'" % documents_dir) if os.path.isdir(documents_dir) else ""),
            )
        )

    # Errors like 'ERROR: role "u_dhis2" does not exist' are expected
    # and safe to ignore.

    # We could skip the "ssh hostname_remote" part if we can access the
    # remote DB from the local machine, but we keep it in case we can't.

    # I'd prefer to do it with postgres custom format and pg_restore:
    #    dump = ("pg_dump -d '%s' --format custom --clean --no-owner %s" %
    #            (db_remote, exclude))
    #    user = db_local[len('postgresql://'):].split(':')[0]
    #    run("ssh %s %s | pg_restore -d '%s' --no-owner --role %s" %
    #        (hostname_remote, dump, db_local, user))
    # but it causes problems with the user role.


def empty_db(db_local):
    "Empty the contents of database"
    db_name = db_local.split("/")[-1]
    postgis_elements = {
        "geography_columns",
        "geometry_columns",
        "raster_columns",
        "raster_overviews",
        "spatial_ref_sys",
        "pg_stat_statements",
    }

    with psycopg2.connect(db_local) as conn:
        with conn.cursor() as cur:

            def fetch(name):
                prefix = {"views": "table", "tables": "table", "sequences": "sequence"}[name]
                cur.execute(
                    "SELECT %(prefix)s_name FROM information_schema.%(name)s "
                    "WHERE %(prefix)s_schema='public' "
                    "AND %(prefix)s_catalog='%(db_name)s'"
                    % {"prefix": prefix, "name": name, "db_name": db_name}
                )
                results_tuples = cur.fetchall()
                results = set(list(zip(*results_tuples))[0] if results_tuples else [])

                return results - postgis_elements

            def drop(name):
                xs = fetch(name)
                log("Dropping %d %s..." % (len(xs), name))
                kind = name[:-1].upper()  # "tables" -> "TABLE"
                for x in xs:
                    cur.execute("DROP %s IF EXISTS %s CASCADE" % (kind, x))
                    conn.commit()

            drop("views")
            drop("tables")
            drop("sequences")

            # If we had permissions, it would be as simple as a
            #   DROP DATABASE dbname
            #   CREATE DATABASE dbname
            # but we may not have those permissions.


def run_sql(cfg, args):
    if is_local_tomcat(cfg):
        for fname in args.post_sql:
            run("psql -d '%s' < '%s'" % (args.db_local, fname))


if __name__ == "__main__":
    main()
