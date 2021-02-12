import sys


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


def remove(item_list, remove_list):
    "Return list removing the items in remove_list"
    xs_unique = []
    for item in item_list:
        if item not in remove_list:
            xs_unique.append(item)
    return xs_unique


def debug(txt):
    print(txt)
    sys.stdout.flush()