import fileinput
import os
import pathlib
import re
import shutil
import subprocess  # nosec

from setuptools import Command

STELLAR_NEBULA_DEPS = os.path.join(pathlib.Path(__file__).parent.absolute(), 'deps', 'stellar-nebula')
COUCHBASE_ROOT = pathlib.Path(__file__).parent


def build_protostellar_init_file(protos):
    main_modules = []
    admin_modules = []
    for proto in protos:
        tokens = proto.split('/')
        end = len(tokens)-1
        if tokens[end].endswith('_grpc.py') and tokens[end-1] == 'couchbase':
            main_modules.append(tokens[end])
        elif tokens[end].endswith('_grpc.py') and tokens[end-1] == 'admin':
            admin_modules.append(tokens[end])

    lines = [
        'import importlib',
        'import importlib.util',
        'import pathlib',
        'import os',
        '',
        '',
    ]

    for module in main_modules:
        tokens = module.split('.')
        abbrv = tokens[0]
        name = module[:-3]
        lines.append(f'# {abbrv}')
        lines.append(
            f"{abbrv}_grpc_spec = importlib.util.spec_from_file_location('{name}', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', '{module}'))")  # noqa: E501
        lines.append(f"{abbrv}_grpc_module = importlib.util.module_from_spec({abbrv}_grpc_spec)")
        lines.append(f"{abbrv}_grpc_spec.loader.exec_module({abbrv}_grpc_module)")
        lines.append('')

    for module in admin_modules:
        tokens = module.split('.')
        abbrv = f'{tokens[0]}_admin'
        name = module[:-3]
        lines.append(f'# {abbrv}')
        lines.append(
            f"{abbrv}_grpc_spec = importlib.util.spec_from_file_location('{name}', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'admin', '{module}'))")  # noqa: E501
        lines.append(f"{abbrv}_grpc_module = importlib.util.module_from_spec({abbrv}_grpc_spec)")
        lines.append(f"{abbrv}_grpc_spec.loader.exec_module({abbrv}_grpc_module)")
        lines.append('')

    # ps_dir = COUCHBASE_ROOT.joinpath('protostellar')
    ps_dir = COUCHBASE_ROOT.joinpath('new_couchbase', 'protostellar')

    with open(os.path.join(ps_dir.absolute(), '__init__.py'), 'w') as output:
        output.write('\n'.join(lines))


def fix_imports(protos):
    pattern = re.compile(r'^from couchbase\.')
    for proto in protos:
        with fileinput.input(proto, inplace=True) as file:
            for line in file:
                match = re.match(pattern, line)
                if match:
                    new_line = line.replace('from couchbase.', 'from new_couchbase.protostellar.proto.couchbase.')
                    print(new_line, end='')
                else:
                    print(line, end='')


class BuildProtosCommand(Command):
    description = 'build Couchbase Protostellar protobufs'
    user_options = []

    def initialize_options(self) -> None:
        return

    def finalize_options(self) -> None:
        return

    def run(self) -> None:
        try:
            import pkg_resources

            sn_protos = COUCHBASE_ROOT.joinpath('deps', 'stellar-nebula', 'proto', 'couchbase')
            proto_files = []
            for root, _, files in os.walk(sn_protos.absolute()):
                proto_files.extend([os.path.join(root, f) for f in files if f.endswith('.proto')])

            if len(proto_files) == 0:
                print('WARNING:  Unabled to find Couchbase SN proto files.')
                return

            protos_to_build = '\n'.join(proto_files)
            print(f'Building these protos: \n{protos_to_build}')

            # ps_proto_dir = COUCHBASE_ROOT.joinpath('protostellar', 'proto')
            ps_proto_dir = COUCHBASE_ROOT.joinpath('new_couchbase', 'protostellar', 'proto')
            # clear our old protos
            shutil.rmtree(ps_proto_dir.absolute())
            os.makedirs(ps_proto_dir.absolute())
            proto_include = pkg_resources.resource_filename('grpc_tools', '_proto')
            contrib_dir = COUCHBASE_ROOT.joinpath('deps', 'stellar-nebula', 'contrib', 'googleapis')

            for proto in proto_files:
                grpc_args = ['python',
                             '-m',
                             'grpc_tools.protoc',
                             f'--proto_path={proto_include}',
                             f'--proto_path={contrib_dir.absolute()}',
                             f'--proto_path={sn_protos.parent.absolute()}',
                             f'--python_out={ps_proto_dir.absolute()}',
                             f'--grpc_python_out={ps_proto_dir.absolute()}',
                             proto, ]
                subprocess.check_call(grpc_args)  # nosec

            built_protos = []
            for root, _, files in os.walk(ps_proto_dir.absolute()):
                built_protos.extend([os.path.join(root, f) for f in files if f.endswith('.py')])

            built_output = '\n'.join(built_protos)
            print(f'Built protos: \n{built_output}')
            # 2 built *.py files for each *.proto file
            if len(proto_files)*2 == len(built_protos):
                print('Successfully built Couchbase SN proto files.')
                build_protostellar_init_file(built_protos)
                print('Successfullly built Protostellar __init__.py.')
                fix_imports(built_protos)
                print('Successfullly fixed stub imports.')
            else:
                print('WARNING:  Unable to build all Couchbase SN proto files.')

        except ImportError:
            print('Could not import grpc_tools.protoc')
