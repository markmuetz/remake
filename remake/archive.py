class Archive:
    def __init__(
        self,
        name,
        repo,
        remakefile,
        author,
        email,
        archive_loc,
    ):
        self.name = name
        self.repo = repo
        self.remakefile = remakefile
        self.author = author
        self.email = email
        self.archive_loc = archive_loc
