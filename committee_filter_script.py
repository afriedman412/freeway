import pandas as pd
from src.config import BASE_URL
from src.src import qry


def committee_search(query):
    url = "/".join([BASE_URL, 'committees', f'search.json?query={query}'])
    r = qry(url)
    assert r.status_code == 200, f"bad status code: {r.status_code}"
    return r.json()['results']


def writeo(name, id):
    print(' '.join(['*** adding', name, id]) + "\n\n")
    with open("ff_pac_ids.csv", "a+") as f:
        f.write(",".join([name, id, "\n"]))
    return


offset = 0

fff = pd.read_csv("ff_remote_filtered.csv")
fffi = iter(
    fff[
        ['search_query', 'name', 'id']
    ].dropna().to_dict('records')[offset:])


for n, i in enumerate(fffi):
    print("z to add, x to pass, q to quit")
    add = input("\n".join(
        [i['search_query'], i['name'], i['id'], str(n+offset)]
    ) + "\n")
    if add == 'z':
        writeo(i['name'], i['id'])
    elif add == 'x':
        continue
    elif add == 'q':
        break
    else:
        query = i['name'].replace("PAC", "")
        while True:
            r = committee_search(query)
            if not r:
                query = input("No results. Alt query?\n")
                if query == 'x':
                    break
            else:
                break
        for r_ in r:
            print("enter to add, x to pass")
            add = input(" / ".join([r_['name'], r_['id']]) + "\n")
            if add == 'x':
                continue
            else:
                writeo(r_['name'], r_['id'])
