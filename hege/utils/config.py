import json
import os
import sys

class Config:
    __config = None

    @staticmethod
    def load(fname=None):
        if fname is None:
            fnames = ["/app/config.json", "./config.json"]  
        else:
            fnames = [fname]

        for config_fname in fnames:
            if os.path.exists(config_fname):
                with open(config_fname, "r") as f:
                    Config.__config = json.load(f)
                    break

        if Config.__config is None:
            sys.exit('Failed to load configuration file!')

    @staticmethod
    def get(name):
        return Config.__config[name]

