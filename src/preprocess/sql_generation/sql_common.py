def write(f, text):
    # print(text)
    f.write(text)


def write_or(f):
    write(f, " or \n")


def write_end_of_sentence(f):
    write(f, " );\n")


def convert_to_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "(" + ", ".join(["'{}'".format(uid) for uid in list_uid]) + ")"


def convert_to_possible_paths_in_sql_format(list_uid):
    if len(list_uid) == 0:
        return ""
    return "and ( path like " + " or path like  " \
                                "".join(["'%{}%'".format(uid) for uid in list_uid]) + \
        ")".replace("(or", " ")
