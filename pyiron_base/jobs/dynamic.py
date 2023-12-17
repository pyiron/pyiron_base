import os
import json
import importlib.util
from pyiron_base.state import state


def _get_class_path_lst(prefix="templates"):
    path_lst = [
        os.path.abspath(os.path.expanduser(os.path.join(p, prefix)))
        for p in state.settings.resource_paths
        if os.path.exists(p) and prefix in os.listdir(p)
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


def _init_constructor_script(class_name, script_path, input_dict):
    def __init__(self, project, job_name):
        super(self.__class__, self).__init__(project, job_name)
        self.__name__ = class_name
        self.input.update(input_dict)
        self.script_path = script_path

    return __init__


def _get_write_input(script):
    def write_input(self):
        super().write_input()
        script.write_input(
            working_directory=self.working_directory, input_dict=self.input.to_builtin()
        )

    return write_input


def _get_collect_output(script):
    def collect_output(self):
        self.output.update(
            script.collect_output(working_directory=self.working_directory)
        )
        self.to_hdf()

    return collect_output


def _get_script(cp):
    spec = importlib.util.spec_from_file_location(
        "script", os.path.join(cp, "script.py")
    )
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    return script


def _init_constructor_dynamic(class_name, module, input_dict):
    from pyiron_base.jobs.job.extension.executable import Executable

    def __init__(self, project, job_name):
        super(self.__class__, self).__init__(project, job_name)
        self.__name__ = class_name
        self.input.update(input_dict)
        self._executable = Executable(
            codename=class_name,
            module=module,
            path_binary_codes=None,
        )

    return __init__


def class_constructor(cp):
    if "templates" in cp:
        from pyiron_base.jobs.script import ScriptJob

        class_name = os.path.basename(cp)
        script_path = os.path.join(cp, "script.py")
        input_dict = _load_input(path=cp)
        return type(
            class_name,
            (ScriptJob,),
            {
                "__init__": _init_constructor_script(
                    class_name=class_name,
                    script_path=script_path,
                    input_dict=input_dict,
                )
            },
        )
    elif "dynamic" in cp:
        from pyiron_base.jobs.job.template import TemplateJob

        class_name = os.path.basename(cp)
        input_dict = _load_input(path=cp)
        script = _get_script(cp=cp)
        return type(
            class_name,
            (TemplateJob,),
            {
                "__init__": _init_constructor_dynamic(
                    class_name=class_name,
                    module=os.path.join(
                        os.path.basename(os.path.dirname(cp)), os.path.basename(cp)
                    ),
                    input_dict=input_dict,
                ),
                "write_input": _get_write_input(script=script),
                "collect_output": _get_collect_output(script=script),
            },
        )


def _get_template_classes():
    return {
        os.path.basename(cp): cp
        for cp in _get_class_path_lst(prefix="templates")
        + _get_class_path_lst(prefix="dynamic")
    }


def create_new_job_type(class_name, input_dict, write_input_funct, collect_output_funct, executable_str):
    from pyiron_base.jobs.job.template import TemplateJob

    def _get_write_input_func(write_input_funct):
        def write_input(self):
            write_input_funct(
                working_directory=self.working_directory, input_dict=self.input.to_builtin()
            )

        return write_input

    def _init_constructor_dynamic_func(class_name, input_dict, executable_str):
        def __init__(self, project, job_name):
            super(self.__class__, self).__init__(project, job_name)
            self.__name__ = class_name
            self.input.update(input_dict)
            self.executable = executable_str

        return __init__

    def _get_collect_output_func(collect_output_funct):
        def collect_output(self):
            self.output.update(
                collect_output_funct(working_directory=self.working_directory)
            )
            self.to_hdf()

        return collect_output

    return type(
        class_name,
        (TemplateJob,),
        {
            "__init__": _init_constructor_dynamic_func(
                class_name=class_name,
                input_dict=input_dict,
                executable_str=executable_str,
            ),
            "write_input": _get_write_input_func(write_input_funct=write_input_funct),
            "collect_output": _get_collect_output_func(collect_output_funct=collect_output_funct),
        },
    )


JOB_DYN_DICT = _get_template_classes()
