"""some yams command line tools"""

import sys
from os.path import exists, join, dirname
from tempfile import NamedTemporaryFile
from subprocess import Popen
from traceback import extract_tb

from logilab.common.configuration import Configuration

from yams import __version__ as version
from yams import schema2dot
from yams.reader import SchemaLoader
from typing import Optional, Union


def _error(
    file: Optional[str] = None, line: Optional[Union[int, str]] = None, msg: str = ""
) -> None:
    if file is None:
        file = sys.argv[1]
    if line is None:
        line = ""
    else:
        line = str(line)
    print(":".join(("E", file, line, msg)), file=sys.stderr)


def check_schema() -> int:
    config = Configuration(
        usage="yams-check [[[...] deps] deps] apps",
        doc="Check the schema of an application.",
        version=version,
    )

    dirnames = config.load_command_line_configuration()

    if not dirnames:
        print(config.help(), file=sys.stderr)

        return 2

    for dir_ in dirnames:
        assert exists(dir_), dir_

    try:
        SchemaLoader().load(dirnames)

        return 0
    except Exception as ex:
        tb_offset = getattr(ex, "tb_offset", 0)
        _, _, traceback = sys.exc_info()
        filename, lineno, _, _ = extract_tb(traceback)[-1 - tb_offset]

        if hasattr(ex, "schema_files"):
            # mypy: "Exception" has no attribute "schema_files"
            # but we've added it above in the exception chain and we test it before
            filename = ", ".join(ex.schema_files)  # type: ignore

        _error(filename, lineno, "%s -> %s" % (ex.__class__.__name__, ex))

        return 2


def schema_image() -> int:
    options = [
        (
            "output-file",
            {
                "type": "file",
                "default": None,
                "metavar": "<file>",
                "short": "o",
                "help": "output image file",
            },
        ),
        (
            "viewer",
            {
                "type": "string",
                "default": "rsvg-view",
                "short": "w",
                "metavar": "<cmd>",
                "help": "command use to view the generated file (empty for none)",
            },
        ),
        (
            "lib-dir",
            {
                "type": "string",
                "short": "L",
                "metavar": "<dir>",
                "help": "directory to look for schema dependancies",
            },
        ),
    ]

    config = Configuration(
        options=options,
        usage="yams-view [-L <lib_dir> | [[[...] deps] deps]] apps",
        version=version,
    )

    dirnames = config.load_command_line_configuration()
    lib_dir = config["lib-dir"]

    assert lib_dir is not None

    if lib_dir is not None:
        app_dir = dirnames[-1]

        from cubicweb.cwconfig import CubicWebConfiguration as cwcfg

        packages = cwcfg.expand_cubes(dirnames)
        packages = cwcfg.reorder_cubes(packages)
        packages = [pkg for pkg in packages if pkg != app_dir]
    elif False:
        glob = globals().copy()

        exec(open(join(app_dir, "__pkginfo__.py")).read(), glob)

        # dirnames = [ join(lib_dir,dep) for dep in glob['__use__']]+dirnames
        packages = [dep for dep in glob["__use__"]]

    for dir_ in dirnames:
        assert exists(dir_), dir_

    import cubicweb

    cubicweb_dir = dirname(cubicweb.__file__)

    schm_ldr = SchemaLoader()

    class MockConfig:
        def packages(self):
            return packages

        def packages_path(self):
            return packages

        def schemas_lib_dir(self):
            return join(cubicweb_dir, "schemas")

        # def apphome(self):
        #    return lib_dir
        apphome = dirnames[0]

        def appid(self):
            "bob"

    print(MockConfig().packages())

    # mypy: Argument 1 to "load" of "SchemaLoader" has incompatible type "MockConfig";
    # mypy: expected "Sequence[Tuple[Any, str]]"
    # mocking
    schema = schm_ldr.load(MockConfig())  # type: ignore

    out, viewer = config["output-file"], config["viewer"]

    if out is None:
        tmp_file = NamedTemporaryFile(suffix=".svg")
        out = tmp_file.name

    schema2dot.schema2dot(
        schema,
        out,  # size=size,
        skiptypes=("Person", "AbstractPerson", "Card", "AbstractCompany", "Company", "Division"),
    )

    if viewer:
        p = Popen((viewer, out))
        p.wait()

    return 0
