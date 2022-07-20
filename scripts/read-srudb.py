import argparse

from dissect.esedb.tools import sru


def main():
    parser = argparse.ArgumentParser(description="dissect.esedb")
    parser.add_argument("input", help="SRU database to read")
    parser.add_argument("-p", "--provider", help="filter records from this provider")
    args = parser.parse_args()

    with open(args.input, "rb") as fh:
        parser = sru.SRU(fh)

        if args.provider in sru.NAME_TO_GUID_MAP:
            for e in parser.get_table_entries(table_name=args.provider):
                print(e)
        elif args.provider:
            for e in parser.get_table_entries(table_guid=args.provider):
                print(e)
        else:
            for e in parser.entries():
                print(e)


if __name__ == "__main__":
    main()
