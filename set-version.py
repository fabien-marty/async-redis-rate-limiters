import argparse
import re

version1_regex = re.compile(r"^version[\s]*=")
version2_regex = re.compile(r"^VERSION[\s]*=")


def update_version_in_file(file: str, version: str):
    with open(file) as f:
        c = f.read()

    lines = []
    for line in c.splitlines():
        if version1_regex.match(line):
            lines.append(f'version = "{version}"')
        elif version2_regex.match(line):
            lines.append(f'VERSION = "{version}"')
        else:
            lines.append(line)

    with open(file, "w") as g:
        g.write("\n".join(lines))


def update_version(version: str, files: list[str]):
    for file in files:
        update_version_in_file(file, version)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update version strings in files. By default, extracts version from git."
    )
    parser.add_argument(
        "version",
        help="Version to set",
    )
    parser.add_argument("files", nargs="+", help="Files to update version in")
    args = parser.parse_args()

    update_version(args.version, args.files)
