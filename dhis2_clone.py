#!/usr/bin/env python3

"""
Clone a dhis2 installation from another server.
"""
import errno
import shutil
import subprocess
import sys
import os
import re
import time
import json
import argparse
from contextlib import contextmanager
from subprocess import Popen

import psycopg2

from src.common.config import Config
from src.preprocess import preprocess
from src.postprocess import postprocess

TIME = time.strftime("%Y-%m-%d_%H%M")
COLOR = True

# Keeps track of every phase run through step(), in order, for the final
# summary. Each item is [name, status, seconds].
STEPS = []


@contextmanager
def step(name):
    "Wrap a phase of the clone process, logging START/OK/FAILED and duration."
    log(f"==== START: {name} ====")
    t0 = time.time()
    entry = [name, "FAILED", 0.0]
    STEPS.append(entry)
    error = ""
    try:
        yield
        entry[1] = "OK"
    except BaseException as e:
        error = f" - {e}"
        raise
    finally:
        entry[2] = time.time() - t0
        log(f"==== {entry[1]}: {name} ({entry[2]:.1f}s){error} ====")


def print_summary():
    if not STEPS:
        return
    log("==== SUMMARY ====")
    for name, status, seconds in STEPS:
        log(f"{status:<6} {name:<30} {seconds:6.1f}s")


def main():
    global COLOR

    args = get_args()

    if args.no_color or not os.isatty(sys.stdout.fileno()):
        COLOR = False

    cfg = get_config(args.config, args.update_config)

    if args.use_backup:
        check_use_backup(cfg["hostname_remote"], args.use_backup)

    pre_api_version, post_api_version = get_api_version(args, cfg)

    Config(pre_api_version, post_api_version)

    if args.update_config:
        update_config(args.config)
        sys.exit()

    try:
        if not args.manual_restart:
            with step("stop_tomcat"):
                stop_tomcat(cfg, args)

        if not args.no_backups:
            with step("backup_db"):
                backup_db(cfg, args)
            with step("backup_war"):
                backup_war(cfg)

        if not args.no_webapps:
            with step("get_webapps"):
                get_webapps(cfg)

        if not args.no_db:
            with step("get_db"):
                get_db(cfg, args)

        if args.no_preprocess:
            log("No preprocessing done, as requested.")
        elif "preprocess" in cfg:
            if cfg["pre_sql_dir"]:
                with step("preprocess"):
                    preprocess.preprocess(cfg["preprocess"], cfg["departments"], cfg["pre_sql_dir"])
                    add_preprocess_sql_file(args, cfg)
            else:
                log("pre_sql_dir not exist in config file")
        else:
            log("No detected preprocessing rules, skipping.")

        if args.post_sql:
            with step("run_sql"):
                run_sql(cfg, args)

        if args.post_clone_scripts:
            with step("post_clone_scripts (pre-tomcat)"):
                execute_scripts(cfg, args)
        if not args.keep_temp and is_local_d2docker(cfg):
            d2_docker_tmp_dir = cfg["server_dir_local"]
            # Only the d2-docker files are truly temporary files (Tomcat files shouldn't be deleted).
            if os.path.exists(d2_docker_tmp_dir) and os.path.isdir(d2_docker_tmp_dir):
                for item in os.listdir(d2_docker_tmp_dir):
                    item_path = os.path.join(d2_docker_tmp_dir, item)
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.unlink(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)

        if not args.manual_restart:
            with step("start_tomcat"):
                start_tomcat(cfg, args)
            do_postprocess(cfg, args)

            if args.post_clone_scripts:
                with step("post_clone_scripts (post-tomcat)"):
                    execute_scripts(cfg, args, is_post_tomcat=True)
        else:
            log("Server not started automatically, as requested.")
            do_postprocess(cfg, args)
    finally:
        print_summary()


def do_postprocess(cfg, args):
    if args.no_postprocess:
        log("No postprocessing done, as requested.")
    elif "api_local_url" in cfg and "postprocess" in cfg:
        import_dir = cfg.get("post_process_import_dir", None)
        timeout = cfg.get("timeout", 900)
        with step("postprocess"):
            postprocess.postprocess(cfg["api_local_url"], args.api_local_username,
                                    args.api_local_password, cfg["postprocess"], import_dir, timeout)
    else:
        log("No postprocessing done.")


def get_api_version(args, cfg):
    supported_versions = ["2.34", "2.36", "2.38", "2.41", "2.42", "34", "36", "38", "41", "42"]
    pre_api_version = None
    post_api_version = None

    if args.pre_api is not None:
        pre_api_version = args.pre_api
    if args.post_api is not None:
        post_api_version = args.post_api

    # Read versions from the config if not provided as a parameter.
    if pre_api_version is None:
        pre_api_version = cfg["pre_api"]
    if post_api_version is None:
        post_api_version = cfg["post_api"]

    if args.pre_api is not None and args.pre_api not in supported_versions:
        print("ERROR: Invalid pre api version given as param")
        sys.exit()
    if args.post_api is not None and args.post_api not in supported_versions:
        print("ERROR: Invalid post api version given as param")
        sys.exit()

    if "pre_api" in cfg and cfg["pre_api"] not in supported_versions:
        print("ERROR: Invalid pre api version in config file")
        sys.exit()

    if "post_api" in cfg and cfg["post_api"] not in supported_versions:
        print("ERROR: Invalid post api version in config file")
        sys.exit()

    if args.pre_api in supported_versions:
        pre_api_version = args.pre_api
    if args.post_api in supported_versions:
        post_api_version = args.post_api

    print("Loaded " + pre_api_version + " api version for pre api calls")
    print("Loaded " + post_api_version + " api version for post api calls")
    return pre_api_version, post_api_version


def add_preprocess_sql_file(args, cfg):
    if is_local_tomcat(cfg):
        args.post_sql.append(os.path.join(cfg["pre_sql_dir"], preprocess.get_file()))
    elif is_local_d2docker(cfg):
        if args.post_sql:
            preprocess.move_file(os.path.join(cfg["pre_sql_dir"], preprocess.get_file()),
                                 os.path.join(args.post_sql[0], preprocess.get_file()))
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
    add("--strict-sql", action="store_true", help="stop the sql script on first fail and show in the log")
    add("--pre-api", help="Pre Api calls compatible versions: 2.34 / 2.36 / 2.38 / 2.41 (default: 2.36)")
    add("--post-api", help="Post Api calls compatible versions: 2.34 / 2.36 / 2.38 / 2.41 (default: 2.36)")
    add("--keep-temp",  action="store_true", help="Preserve temporary d2-docker files for cloning the instance")
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
    add("--use-backup", type=str, help="Path to remote backup file to use insted of making a remote pg_dump")
    return parser.parse_args()


def check_use_backup(remote, path):
    status = os.system(f'ssh {remote} [ -f "{path}" ]')
    exit_code = os.waitstatus_to_exitcode(status)
    if exit_code != 0:
        print(f"ERROR: remote backup file {path} does not exists.")
        sys.exit(exit_code)


def get_config(fname, update):
    "Return dict with the options read from configuration file"
    log(f"Reading from config file {fname} ...")
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
        sys.exit(f"Error reading config file {fname}: {e}")
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
        log(f"Writing updated configuration in {fname_new}")
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


def run(cmd, label=None, capture=True):
    log(cmd)
    if not capture:
        exit_code = subprocess.run(cmd, shell=True).returncode
        if exit_code != 0:
            log(f"FAILED (exit {exit_code}): {label or cmd}")
            sys.exit(exit_code)
        log(f"OK: {label or cmd}")
        return exit_code
    prefix = f"  [{label}] " if label else "  "
    with Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1
    ) as p:
        for line in p.stdout:
            print(prefix + line.rstrip("\n"))
        sys.stdout.flush()
        exit_code = p.wait()
    if exit_code != 0:
        log(f"FAILED (exit {exit_code}): {label or cmd}")
        sys.exit(exit_code)
    log(f"OK: {label or cmd}")
    return exit_code


def log(txt):
    clean_auth = re.sub(
        r"(--auth\s+')(.*?):(.*?)(')",
        "--auth user:PASSWORD",
        txt
    )
    clean_txt = re.sub(r"://(.*?):(.*?)@", "://\\1:PASSWORD@", clean_auth)
    out = f"[{time.strftime('%Y-%m-%d %T')}] {clean_txt}"
    print((magenta(out) if COLOR else out))
    sys.stdout.flush()


def magenta(txt):
    return f"\x1b[35m{txt}\x1b[0m"


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

    base_url = cfg["api_local_url"].replace("://", f"://{args.api_local_username}:{args.api_local_password}@")

    for script in sorted(filter(is_script, files_list)):
        run(f'"{dirname}/{script}" "{base_url}"', label=script)


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
        run(f'"{server_path}/bin/startup.sh"', capture=False)
    elif is_local_d2docker(cfg):
        post_sql = args.post_sql[0] if args.post_sql else None
        deploy_path = cfg.get("local_docker_deploy_path", None)
        server_xml_path = cfg.get("local_docker_server_xml", None)
        dhis_conf_path = cfg.get("local_docker_dhis_conf", None)
        temp_folder = cfg.get("docker_temp_folder", None)
        if post_sql:
            if len(args.post_sql) != 1 or not os.path.isdir(post_sql):
                log("--post-sql for d2-docker requires a single directory")
                return
            elif args.strict_sql:
                add_strict_to_filenames(post_sql)

        post_scripts_dir = cfg.get("local_docker_post_clone_scripts_dir", None)
        api_url = cfg["api_local_url"]

        temp_directory_opt = f"--temp-directory '{temp_folder}'" if temp_folder else ""
        deploy_path_opt = f"--deploy-path '{deploy_path}'" if deploy_path else ""
        server_xml_opt = f"--tomcat-server-xml '{server_xml_path}'" if server_xml_path else ""
        dhis_conf_opt = f"--dhis-conf '{dhis_conf_path}'" if dhis_conf_path else ""
        run_sql_opt = f"--run-sql '{post_sql}'" if post_sql else ""
        run_scripts_opt = f"--run-scripts '{post_scripts_dir}'" if post_scripts_dir else ""
        auth_opt = f"--auth '{args.api_local_username}:{args.api_local_password}'" if api_url else ""

        run(
            f"d2-docker {temp_directory_opt} start {get_local_docker_image(cfg, args, 'start')} "
            f"--port={cfg['local_docker_port']} --detach {deploy_path_opt} {server_xml_opt} "
            f"{dhis_conf_opt} {run_sql_opt} {run_scripts_opt} {auth_opt}",
            capture=False,
        )


def add_strict_to_filenames(post_sql):
    for filename in os.listdir(post_sql):
        if '_strict' in filename:
            continue
        old_path = os.path.join(post_sql, filename)
        if os.path.isfile(old_path):
            name, ext = os.path.splitext(filename)
            new_filename = f"{name}_strict{ext}"
            new_path = os.path.join(post_sql, new_filename)
            os.replace(old_path, new_path)
            log(f"Renamed: {old_path} -> {new_path}")


def stop_tomcat(cfg, args):
    if is_local_tomcat(cfg):
        server_path = cfg["server_dir_local"]
        run(f'"{server_path}/bin/shutdown.sh"', capture=False)
    elif is_local_d2docker(cfg):
        run(f"d2-docker stop {get_local_docker_image(cfg, args, 'stop')}", capture=False)


def backup_db(cfg, args):
    backups_dir = cfg["backups_dir"]
    if is_local_tomcat(cfg):
        backup_name = cfg["backup_name"]
        db_local = args.db_local
        backup_file = f"{backups_dir}/{backup_name}_{TIME}.dump"
        run(
            f"pg_dump --file '{backup_file}' --format custom --exclude-schema sys --clean '{db_local}' "
        )
    elif is_local_d2docker(cfg):
        run(f"d2-docker copy {get_local_docker_image(cfg, args, 'stop')} '{backups_dir}'")


def backup_war(cfg):
    if is_local_tomcat(cfg):
        backups_dir = cfg["backups_dir"]
        dir_local = cfg["server_dir_local"]
        war_local = cfg["war_local"]
        backup_file = f"{backups_dir}/{war_local[:-4]}_{TIME}.war"
        run(f'cp "{dir_local}/webapps/{war_local}" "{backup_file}"')
    elif is_local_d2docker(cfg):
        pass


def get_webapps(cfg):
    route_local = cfg["server_dir_local"]
    route_remote = f"{cfg['hostname_remote']}:{cfg['server_dir_remote']}"

    for mandatory, subdir in [[True, "webapps"], [False, "files/apps"], [False, "files/document"], [False, "files/dataValue"]]:
        cmd = f"rsync -avP -LK --delete --relative {route_remote}/./{subdir} {route_local}"
        log(cmd)
        with Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            universal_newlines=True,
            stderr=subprocess.PIPE,
        ) as p:
            stdout, stderr = p.communicate()
        if p.returncode != 0 and mandatory:
            log(f"Mandatory folder {subdir} failed to rsync")
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT) + "\n" + stderr, subdir)

    if is_local_tomcat(cfg):
        war_local = cfg["war_local"]
        war_remote = cfg["war_remote"]

        if war_local != war_remote:
            commands = [
                f'cd "{route_local}/webapps"',
                f'rm -f "{war_local}"',  # the war file itself
                f'mv "{war_remote}" "{war_local}"',
                f'rm -rf "{war_local[:-4]}"',  # the directory
                f'mv "{war_remote[:-4]}" "{war_local[:-4]}"',
            ]
            run("; ".join(commands))


def get_db(cfg, args):
    "Replace the contents of db_local with db_remote"
    exclude = (
        " --exclude-table 'analytics*' --exclude-table 'completeness*' "
        " --exclude-schema sys "
    )
    dir_local = cfg["server_dir_local"]

    if is_local_tomcat(cfg):
        db_remote = args.db_remote

        if args.use_backup:
            dump = f"zcat '{args.use_backup}'"
        else:
            dump = f"pg_dump -U dhis -d '{db_remote}' --no-owner {exclude}"

        db_local = args.db_local
        empty_db(db_local)
        cmd = f"ssh {cfg['hostname_remote']} {dump} | psql -d '{db_local}'"

        run(cmd + " 2>&1 | paste - - - | uniq -c")  # run with more compact output
    elif is_local_d2docker(cfg):
        db_remote = args.db_remote
        dump = f"pg_dump -U dhis -d '{db_remote}' --no-owner {exclude}"
        sql_path = os.path.join(dir_local, "db.sql.gz")
        cmd = f"ssh {cfg['hostname_remote']} {dump} | gzip > {sql_path}"
        run(cmd)
        apps_dir = os.path.join(dir_local, "files", "apps")
        documents_dir = os.path.join(dir_local, "files", "document")
        datavalues_dir = os.path.join(dir_local, "files", "dataValue")
        temp_folder = cfg.get("docker_temp_folder", None)

        temp_directory_opt = f"--temp-directory '{temp_folder}'" if temp_folder else ""
        apps_dir_opt = f"--apps-dir '{apps_dir}'" if os.path.isdir(apps_dir) else ""
        documents_dir_opt = f"--documents-dir '{documents_dir}'" if os.path.isdir(documents_dir) else ""
        datavalues_dir_opt = f"--datavalues-dir '{datavalues_dir}'" if os.path.isdir(datavalues_dir) else ""

        run(
            f"d2-docker {temp_directory_opt} create data {get_local_docker_image(cfg, args, 'stop')} "
            f"--sql={sql_path} {apps_dir_opt} {documents_dir_opt} {datavalues_dir_opt}"
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
                    f"SELECT {prefix}_name FROM information_schema.{name} "
                    f"WHERE {prefix}_schema='public' "
                    f"AND {prefix}_catalog='{db_name}'"
                    f"AND {prefix}_name NOT IN ("
                    "  SELECT objid::regclass::text FROM pg_depend "
                    "  WHERE deptype = 'e'"
                    ")"
                )
                results_tuples = cur.fetchall()
                results = set(list(zip(*results_tuples))[0] if results_tuples else [])

                return results - postgis_elements

            def drop(name):
                xs = fetch(name)
                log(f"Dropping {len(xs)} {name}...")
                kind = name[:-1].upper()  # "tables" -> "TABLE"
                for x in xs:
                    try:
                        cur.execute(f"DROP {kind} IF EXISTS {x} CASCADE")
                        conn.commit()
                    except Exception as e:
                        log(f"Error dropping {kind} {x}: {e}")
                        conn.rollback()
                        sys.exit(1)

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
            run(f"psql -d '{args.db_local}' < '{fname}'", label=os.path.basename(fname))


if __name__ == "__main__":
    main()
