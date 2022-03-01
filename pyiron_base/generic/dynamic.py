import os
import json
from pyiron_base.state import state


def _get_class_path_lst():
    path_lst = [
        os.path.abspath(os.path.expanduser(os.path.join(p, "templates")))
        for p in state.settings.resource_paths
        if os.path.exists(p) and "templates" in os.listdir(p)
    ]
    class_path_lst = []
    for path in path_lst:
        class_path_lst += [
            cp
            for cp, files in [
                [cp, os.listdir(cp)]
                for cp in [os.path.join(path, c) for c in os.listdir(path)]
            ]
            if "script.py" in files and "input.json" in files
        ]
    return class_path_lst


def _load_input(path):
    with open(os.path.join(path, "input.json")) as f:
        return json.load(f)


def _init_constructor(class_name, script_path, input_dict):
    def __init__(self, project, job_name):
        super(self.__class__, self).__init__(project, job_name)
        self.__name__ = class_name
        self.input.update(input_dict)
        self.script_path = script_path

    return __init__


def class_constructor(cp):
    from pyiron_base.job.script import ScriptJob

    class_name = os.path.basename(cp)
    script_path = os.path.join(cp, "script.py")
    input_dict = _load_input(path=cp)
    return type(
        class_name,
        (ScriptJob,),
        {
            "__init__": _init_constructor(
                class_name=class_name, script_path=script_path, input_dict=input_dict
            )
        },
    )


def _get_template_classes():
    return {os.path.basename(cp): cp for cp in _get_class_path_lst()}


JOB_DYN_DICT = _get_template_classes()
