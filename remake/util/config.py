class Config:
    def __init__(self, defaults, allow_other_keys=False):
        self.cfg = {k: ([f'default:{v}'], v) for k, v in defaults.items()}
        self.allow_other_keys = allow_other_keys

    def asdict(self):
        return {k:v[1] for k, v in self.cfg.items()}

    def update(self, setby, updates):
        for k, v in updates.items():
            if k in self.cfg:
                prev_setby, prev_v = self.cfg[k]
                self.cfg[k] = ([f'{setby}:{v}'] + prev_setby, v)
            else:
                if not self.allow_other_keys:
                    raise Exception(f'Trying to add new key {k} and allow_other_keys = False')
                self.cfg[k] = (setby, v)

    def copy(self):
        cp = Config({}, allow_other_keys=self.allow_other_keys)
        cp.cfg = self.cfg.copy()
        return cp

    def get(self, key, default):
        if key in self.cfg:
            return self.cfg[key][1]
        else:
            return default

    def __getitem__(self, key):
        return self.cfg[key][1]

    def print(self):
        for k, (setby, v) in self.cfg.items():
            setby_str = ' > '.join(setby)
            print(f'{k}: {v} ({setby_str})')
