import os
import abc
import json
import tarfile
from hashlib import sha1
from pathlib import Path

from loguru import logger

from ..util import sysrun, get_git_info, git_archive
from ..version import __version__
from .task import Task


def humanbytes(B):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
    B = float(B)
    KB = float(1024)
    MB = float(KB**2)  # 1,048,576
    GB = float(KB**3)  # 1,073,741,824
    TB = float(KB**4)  # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B, 'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.2f} MB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.2f} GB'.format(B / GB)
    elif TB <= B:
        return '{0:.2f} TB'.format(B / TB)


class ArchiveTask(Task):
    def __init__(self, archive_file, archive_path, file_manifest):
        self.archive_file = archive_file
        self.archive_path = archive_path
        self.file_manifest = file_manifest

        class _ArchRule:
            pass

        super().__init__(_ArchRule, {}, {}, {'archive_path': self.archive_path})

    def key(self):
        return sha1((','.join(str(v[1]) for v in self.file_manifest)).encode()).hexdigest()

    def run(self):
        logger.info(f'Archive to: {self.archive_path}')
        total_size = sum([f[3] for f in self.file_manifest])
        # with tarfile.open(archive_path, 'w:gz') as tar:
        with tarfile.open(self.archive_path, 'w') as tar:
            size_archived = 0
            for prepend, path, shorten, size in [
                (prepend, Path(path), shorten, size)
                for prepend, path, shorten, size in self.file_manifest
            ]:
                logger.trace((prepend, path, shorten, size))
                if shorten:
                    tar.add(str(path), str(Path(prepend) / path.name))
                else:
                    if path.is_absolute():
                        # Convert to relative path.
                        tarpath = Path('').joinpath(*path.parts[1:])
                    else:
                        tarpath = path
                    tar.add(str(path), str(Path(prepend) / tarpath))
                size_archived += size
                logger.debug(
                    f'archived {size_archived}/{total_size} ({size_archived / total_size * 100:.1f}%)'
                )


class BaseArchive(abc.ABC):
    @abc.abstractmethod
    def version(self):
        pass


class ArchiveV1(BaseArchive):
    def __init__(
        self,
        name,
        git_repo_url,
        remakefile,
        author,
        email,
        archive_loc,
    ):
        self.name = name
        self.git_repo_url = git_repo_url
        self.remakefile = remakefile
        self.author = author
        self.email = email
        self.archive_loc = archive_loc

        self._version = 'v1'
        self.rmk = None
        self.archive_rules = []

    def version(self):
        return self._version

    def add_remake(self, rmk):
        self.rmk = rmk

    def add(self, archive_rule):
        self.archive_rules.append(archive_rule)

    def archive(self, archive_file, dry_run, executor='SingleprocExecutor'):
        file_manifest = []
        archive_dir = Path(f'.remake/archive/{self.name}').absolute()

        git_info = get_git_info()
        if git_info.is_repo:  # and git_info.status == 'clean':
            orig_cwd = Path.cwd()
            cwd = orig_cwd
            while not (cwd / '.git').exists():
                cwd = cwd.parent
                if str(cwd) == '/':
                    raise Exception('Not a git repo!')
            reldir = orig_cwd.relative_to(cwd)

            os.chdir(cwd)
            git_archive_path = git_archive(
                self.name, 'main', archive_dir / 'code.git_archive.tar.gz'
            )
            os.chdir(orig_cwd)

            file_manifest.append(
                ('archive_code', str(git_archive_path), True, git_archive_path.lstat().st_size)
            )
        else:
            raise Exception('Not a git repo or not clean')

        conda_env_yml = sysrun('conda env export | grep -v "^prefix: "').stdout

        data_files = []
        for archive_rule in self.archive_rules:
            rule = getattr(self.rmk, archive_rule.rule)
            logger.debug(f'archive: {rule}')
            for task in rule.tasks:
                if archive_rule.inputs:
                    for path in task.inputs.values():
                        size = Path(path).lstat().st_size
                        file_manifest.append(('archive_data', str(path), False, size))
                        data_files.append(dict(path=str(path), size=size))

        total_size = sum([f[3] for f in file_manifest])
        archive_metadata = {
            'archive_version': self._version,
            'name': self.name,
            'git_repo_url': self.git_repo_url,
            'remakefile': self.remakefile,
            'author': self.author,
            'email': self.email,
            'total_size': total_size,
            'total_size_human': humanbytes(total_size),
            'reldir': str(reldir),
            'git_info': {
                'hash': git_info.git_hash,
                'describe': git_info.describe,
                'status': git_info.status,
            },
            'remake_info': {
                'version': str(__version__),
            },
        }
        archive_dir = Path(f'.remake/archive/{self.name}')
        archive_dir.mkdir(exist_ok=True, parents=True)

        metadata_file = archive_dir / 'metadata.json'
        metadata_file.write_text(json.dumps(archive_metadata, indent=4))
        file_manifest.append(
            ('archive_metadata', str(metadata_file), True, metadata_file.lstat().st_size)
        )

        conda_env_file = archive_dir / 'conda_env.yml'
        conda_env_file.write_text(conda_env_yml)
        file_manifest.append(
            ('archive_metadata', str(conda_env_file), True, conda_env_file.lstat().st_size)
        )

        data_files_file = archive_dir / 'data_files.json'
        data_files_file.write_text(json.dumps(data_files, indent=4))
        file_manifest.append(
            ('archive_metadata', str(data_files_file), True, data_files_file.lstat().st_size)
        )
        file_manifest = sorted(set(file_manifest), key=lambda x: x[1])

        logger.info(f'Total file size: {humanbytes(total_size)}')

        if not dry_run:
            # archive_path = Path(f'{self.archive_loc}') / f'{self.name}.remake_archive.tar.gz'
            archive_path = Path(f'{self.archive_loc}') / f'{self.name}.remake_archive.tar'
            archive_task = ArchiveTask(archive_file, archive_path, file_manifest)
            executor = self.rmk._get_executor(executor)
            executor.run_tasks([archive_task])


class ArchiveV1Rule:
    def __init__(self, rule, inputs=None):
        self.rule = rule
        self.inputs = inputs


def restore(archive_file, data_dir=None):
    logger.info(f'Restoring code and data from: {archive_file}')
    # with tarfile.open(f'{archive_file}', 'r:gz') as tar:
    with tarfile.open(f'{archive_file}', 'r') as tar:
        logger.trace(f'read metadata')
        archive_metadata = json.loads(tar.extractfile('archive_metadata/metadata.json').read())
        logger.debug(archive_metadata)

        name = Path(archive_metadata['name'])
        logger.info(f'Restore code to: {name}')

        if data_dir is None:
            reldir = name / Path(archive_metadata['reldir'])
        else:
            reldir = Path(data_dir)
        logger.info(f'Restore data to: {reldir}')
        reldir.mkdir(parents=True)

        for tarname in tar.getnames():
            print(tarname)
            if tarname.startswith('archive_data'):
                # TODO: HACKY
                relname = reldir / tarname[13:]
                relname.parent.mkdir(exist_ok=True, parents=True)
                logger.trace(f'restoring file: {relname}')
                with open(relname, 'wb') as fp:
                    fp.write(tar.extractfile(tarname).read())

        envdir = name / 'env'
        envdir.mkdir(parents=True)
        envfile = envdir / 'conda_env.yml'
        with open(envfile, 'wb') as fp:
            fp.write(tar.extractfile('archive_metadata/conda_env.yml').read())

        with open('code.git_archive.tar.gz', 'wb') as fp:
            fp.write(tar.extractfile('archive_code/code.git_archive.tar.gz').read())
    with tarfile.open('code.git_archive.tar.gz', 'r:gz') as tar:
        tar.extractall(name)
    Path('code.git_archive.tar.gz').unlink()
    logger.info('All files restored')
    remakefile_path = name / Path(archive_metadata['reldir']) / archive_metadata['remakefile']
    logger.info(f'The remakefile is located in: {remakefile_path}')
    logger.warning(f'You will need to edit your remakefile to point to the new paths!')
