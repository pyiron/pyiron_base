"""
Decorators to manipulate the __dir__ of user facing classes to show UI functions only.
"""

def add_user_facing_attr(inst, name):
    inst._user_dir.append(name)

def user_facing(method):
    method.user_facing = True
    return method

def mangle_dir(cls):
    dir_list = getattr(cls, "_user_dir", [])
    for name, desc in cls.__dict__.items():
        if hasattr(desc, "user_facing"):
            dir_list.append(name)
    cls._user_dir = dir_list
    return cls
