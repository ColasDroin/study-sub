# ==================================================================================================
# --- Imports
# ==================================================================================================
# Third party imports
import yaml


# ==================================================================================================
# --- Functions
# ==================================================================================================
def generate_run_file(
    job_folder, job_name, setup_env_script, generation_number, tree_path, l_keys, htc=False
):
    # if htc:
    #     file_str = _generate_run_file_htc(job_folder, job_name, setup_env_script, generation_number)
    # else:
    file_str = _generate_run_file(job_folder, job_name, setup_env_script)

    # Tag the job as finished with all keys from l_keys
    file_str += f"""
# Ensure job run was successful and tag as finished
if [ $? -eq 0 ]; then
    python -m study_sub.scripts.log_finish {tree_path} {' '.join(l_keys)}
fi
"""

    return file_str


def _generate_run_file(job_folder, job_name, setup_env_script):
    return (
        "#!/bin/bash\n"
        + f"source {setup_env_script}\n"
        + f"cd {job_folder}\n"
        + f"python {job_name} > output_python.txt 2> error_python.txt\n"
    )


# def _generate_run_file_htc(job_folder, job_name, setup_env_script, generation_number):
#     if generation_number == 1:
#         # No need to move to HTC as gen 1 is never IO intensive
#         return _generate_run_file(job_folder, job_name, setup_env_script)
#     if generation_number == 2:
#         raise ValueError("Generation 2 htc submission is not supported yet...")
#         # return _generate_run_sh_htc_gen_2(job_folder, job_name, setup_env_script)
#     if generation_number >= 3:
#         print(
#             f"Generation {generation_number} local htc submission is not supported yet..."
#             " Submitting as for generation 1"
#         )
#         return _generate_run_file(job_folder, job_name, setup_env_script)


# ! THIS CODE IS NOT UP TO DATE AND WILL NOT WORK
# def _generate_run_sh_htc_gen_2(job_folder, job_name, setup_env_script):
#     # Get local path and abs path to gen 2
#     abs_path = job_folder
#     local_path = abs_path.split("/")[-1]

#     # Mutate all paths in config to be absolute
#     with open(f"{abs_path}/config.yaml", "r") as f:
#         config = yaml.load(f, Loader=yaml.FullLoader)

#     # Get paths to mutate
#     path_collider = config["config_simulation"]["collider_file"]
#     path_particles = config["config_simulation"]["particle_file"]
#     path_log = config["log_file"]
#     new_path_collider = f"{abs_path}/{path_collider}"
#     new_path_particles = f"{abs_path}/{path_particles}"
#     new_path_log = f"{abs_path}/{path_log}"

#     # Prepare strings for sec
#     path_collider = path_collider.replace("/", "\/")
#     path_particles = path_particles.replace("/", "\/")
#     path_log = path_log.replace("/", "\/")
#     new_path_collider = new_path_collider.replace("/", "\/")
#     new_path_particles = new_path_particles.replace("/", "\/")
#     new_path_log = new_path_log.replace("/", "\/")

#     # Return final run script
#     return (
#         f"#!/bin/bash\n"
#         f'source {node.root.parameters["setup_env_script"]}\n'
#         # Copy config gen 1
#         f"cp -f {abs_path}/../config.yaml .\n"
#         # Copy config gen 2 in local path
#         f"mkdir {local_path}\n"
#         f"cp -f {abs_path}/config.yaml {local_path}\n"
#         f"cd {local_path}\n"
#         # Mutate the paths in config to be absolute
#         f'sed -i "s/{path_collider}/{new_path_collider}/g" config.yaml\n'
#         f'sed -i "s/{path_particles}/{new_path_particles}/g" config.yaml\n'
#         f'sed -i "s/{path_log}/{new_path_log}/g" config.yaml\n'
#         # Run the job
#         f"python {node.get_abs_path()}/{python_command} > output_python.txt 2>"
#         " error_python.txt\n"
#         # Delete the config so it's not copied back
#         f"rm -f ../config.yaml\n"
#         # Copy back output
#         f"cp -f *.txt *.parquet *.yaml {abs_path}\n"
#     )
