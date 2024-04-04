# ==================================================================================================
# --- Imports
# ==================================================================================================
import copy

# Standard library imports
import os
import subprocess
from pathlib import Path
from typing import Self

import psutil

# Third party imports
from study_gen._nested_dicts import nested_get

# Local imports
from .submission_statements import HTC, HTCDocker, LocalPC, Slurm, SlurmDocker


# ==================================================================================================
# --- Class for job submission
# ==================================================================================================
class ClusterSubmission:
    def __init__(
        self: Self,
        study_name: str,
        l_jobs_to_submit: list[str],
        dic_all_jobs: dict,
        dic_tree: dict,
        path_submission_file: str,
    ):
        self.study_name = study_name
        self.l_jobs_to_submit = l_jobs_to_submit
        self.dic_all_jobs = dic_all_jobs
        self.dic_tree = dic_tree
        self.path_submission_file = path_submission_file

        # TODO
        # ! Adapt this piece of code where it is needed
        # # GPU configuration (for HTC)
        # if config["context"] in ["cupy", "opencl"] and self.run_on in [
        #     "htc",
        #     "htc_docker",
        #     "slurm",
        #     "slurm_docker",
        # ]:
        #     self.request_GPUs = 1
        #     self.slurm_queue_statement = ""
        # else:
        #     self.request_GPUs = 0
        #     self.slurm_queue_statement = "#SBATCH --partition=slurm_hpc_acc"

        # TODO
        # ! Also adapt this piece of code where it is needed
        # # Path to singularity image
        # if "container_image" in self.dic_tree:
        #     self.path_image = self.dic_tree["container_image"]

        # elif self.submission_type in ["slurm_docker", "htc_docker"]:
        #     raise ValueError(
        #         "Error: container_image must be defined in tree for slurm_docker and"
        #         " htc_docker"
        #     )
        # else:
        #     # Needs to be defined, but irrelevant for local_pc, slurm and htc
        #     self.path_image = ""

    # Getter for dic_id_to_job
    @property
    def dic_id_to_job(self):
        dic_id_to_job = {}
        found_at_least_one = False
        for job in self.l_jobs_to_submit:
            l_keys = self.dic_all_jobs[job]["l_keys"]
            subdic_job = nested_get(self.dic_tree, l_keys)
            if "id_sub" in subdic_job:
                dic_id_to_job[subdic_job["id_sub"]] = job
                found_at_least_one = True

        return dic_id_to_job if found_at_least_one else None

    # Setter for dic_id_to_job
    @dic_id_to_job.setter
    def dic_id_to_job(self, dic_id_to_job):
        assert isinstance(dic_id_to_job, dict)
        # Ensure all ids are integers
        dic_id_to_job = {int(id_job): job for id_job, job in dic_id_to_job.items()}

        # Update the tree
        for job in self.l_jobs_to_submit:
            l_keys = self.dic_all_jobs[job]["l_keys"]
            subdic_job = nested_get(self.dic_tree, l_keys)
            if "id_sub" in subdic_job and subdic_job["id_sub"] not in dic_id_to_job:
                del subdic_job["id_sub"]
            elif "id_sub" not in subdic_job and subdic_job["id_sub"] in dic_id_to_job:
                subdic_job["id_sub"] = dic_id_to_job[subdic_job["id_sub"]]
            # Else all is consistent

        # TODO WHEN DEBUGGING: Ensure that modifying subdic indeed modifies the dic_tree

    def _update_dic_id_to_job(self, running_jobs, queuing_jobs):
        # Look for jobs in the dictionnary that are not running or queuing anymore
        set_current_jobs = set(running_jobs + queuing_jobs)
        if self.dic_id_to_job is not None:
            dic_id_to_job = self.dic_id_to_job
            for id_job, job in self.dic_id_to_job.items():
                if job not in set_current_jobs:
                    del dic_id_to_job[id_job]

            # Update dic_id_to_job
            self.dic_id_to_job = dic_id_to_job

    def _get_state_jobs(self, verbose=True):
        running_jobs = self.querying_jobs(dic_id_to_job=self.dic_id_to_job, status="running")
        queuing_jobs = self.querying_jobs(dic_id_to_job=self.dic_id_to_job, status="queuing")
        self._update_dic_id_to_job(running_jobs, queuing_jobs)
        if verbose:
            print("Running: \n" + "\n".join(running_jobs))
            print("queuing: \n" + "\n".join(queuing_jobs))
        return running_jobs, queuing_jobs

    # TODO
    # ! Keep going from here
    @staticmethod
    def _get_path_job(path_node):
        path_job = copy.copy(path_node)

        # Fix for path
        if not path_node.endswith("/"):
            path_job += "/"

        # Only get path after master_study
        path_job = path_job.split("master_study")[1]

        return path_job

    @staticmethod
    def _test_node(node, path_job, running_jobs, queuing_jobs):
        # Test if node is running, queuing or completed
        if node.has_been("completed"):
            print(f"{path_job} is already completed.")
        elif path_job in running_jobs:
            print(f"{path_job} is already running.")
        elif path_job in queuing_jobs:
            print(f"{path_job} is already queuing.")
        else:
            return True
        return False

    def _write_sub_files_slurm(self, filename, running_jobs, queuing_jobs, list_of_nodes):
        l_filenames = []
        l_path_jobs = []
        for idx_node, node in enumerate(list_of_nodes):
            # Get path node
            path_node = node.get_abs_path()

            # Get corresponding path job
            path_job = self._get_path_job(path_node)

            # Test if node is running, queuing or completed
            if self._test_node(node, path_job, running_jobs, queuing_jobs):
                filename_node = f"{filename.split('.sub')[0]}_{idx_node}.sub"

                # Write the submission files
                print(f'Writing submission file for node "{path_node}"')
                with open(filename_node, "w") as fid:
                    # ! Careful, I implemented a fix for path due to the temporary home recovery folder
                    to_replace = "/storage-hpc/gpfs_data/HPC/home_recovery"
                    replacement = "/home/HPC"
                    fixed_path = path_node.replace(to_replace, replacement)
                    # update path for sed
                    to_replace = to_replace.replace("/", "\/")
                    replacement = replacement.replace("/", "\/")

                    # Head
                    fid.write(self.dic_submission[self.run_on]["head"](fixed_path))

                    # Mutate path in run.sh and other potentially problematic files
                    fid.write(f"sed -i 's/{to_replace}/{replacement}/' {fixed_path}/run.sh\n")
                    fid.write(f"sed -i 's/{to_replace}/{replacement}/' {fixed_path}/config.yaml\n")

                    # Body
                    fid.write(self.dic_submission[self.run_on]["body"](fixed_path))

                    # Tail
                    fid.write(self.dic_submission[self.run_on]["tail"])

                l_filenames.append(filename_node)
                l_path_jobs.append(path_job)
        return l_filenames, l_path_jobs

    def _write_sub_file(
        self, filename, running_jobs, queuing_jobs, list_of_nodes, write_htc_job_flavour=False
    ):
        # Get submission instructions
        str_head = self.dic_submission[self.run_on]["head"]
        str_body = self.dic_submission[self.run_on]["body"]
        str_tail = self.dic_submission[self.run_on]["tail"]

        # Flag to know if the file can be submitted (at least one job in it)
        ok_to_submit = False

        # Record list of jobs
        l_path_jobs = []

        # Write the submission file
        with open(filename, "w") as fid:
            fid.write(str_head)
            for node in list_of_nodes:
                # Get path node
                path_node = node.get_abs_path()

                # Get corresponding path job
                path_job = self._get_path_job(path_node)

                # Test if node is running, queuing or completed
                if self._test_node(node, path_job, running_jobs, queuing_jobs):
                    print(f'Writing submission command for node "{path_node}"')
                    # Write instruction for submission
                    fid.write(str_body(path_node))

                    # if user has defined a htc_job_flavor in config.yaml otherwise default is "espresso"
                    if write_htc_job_flavour:
                        if "htc_job_flavor" in self.config:
                            htc_job_flavor = self.config["htc_job_flavor"]
                        else:
                            print(
                                "Warning: htc_job_flavor not defined in config.yaml. Using espresso"
                                " as default"
                            )
                            htc_job_flavor = "espresso"
                        fid.write(f'+JobFlavour  = "{htc_job_flavor}"\n')

                    # Add job to list
                    l_path_jobs.append(path_job)

                    # Flag file
                    ok_to_submit = True

            # Tail instruction
            fid.write(str_tail)

        if not ok_to_submit:
            os.remove(filename)

        return ([filename], l_path_jobs) if ok_to_submit else ([], [])

    def _write_sub_files(self, filename, running_jobs, queuing_jobs, list_of_nodes):
        # Slurm docker is a peculiar case as one submission file must be created per job
        if self.run_on == "slurm_docker":
            return self._write_sub_files_slurm(filename, running_jobs, queuing_jobs, list_of_nodes)

        else:
            return self._write_sub_file(
                filename,
                running_jobs,
                queuing_jobs,
                list_of_nodes,
                write_htc_job_flavour=self.run_on in ["htc", "htc_docker"],
            )

    def write_sub_files(self):
        running_jobs, queuing_jobs = self._get_state_jobs(verbose=False)
        l_filenames, l_path_jobs = self._write_sub_files(
            self.path_submission_file, running_jobs, queuing_jobs, self.l_jobs_to_submit
        )
        return l_filenames, l_path_jobs

    def submit(self, l_filenames, l_jobs):
        # Check that the submission file(s) is/are appropriate for the submission mode
        if len(l_filenames) > 1 and self.run_on != "slurm_docker":
            raise ValueError(
                "Error: Multiple submission files should not be implemented for this submission"
                " mode"
            )
        if len(l_filenames) == 0:
            print("No job being submitted.")

        # Submit
        dic_id_to_job_temp = {}
        idx_submission = 0
        for filename in l_filenames:
            if self.run_on not in self.dic_submission:
                raise ValueError(f"Error: {self.run_on} is not a valid submission mode")
            if self.run_on == "local_pc":
                os.system(self.dic_submission[self.run_on]["submit_command"](filename))
            else:
                process = subprocess.run(
                    self.dic_submission[self.run_on]["submit_command"](filename).split(" "),
                    capture_output=True,
                )
                output = process.stdout.decode("utf-8")
                output_error = process.stderr.decode("utf-8")
                if "ERROR" in output_error:
                    raise RuntimeError(f"Error in submission: {output}")
                for line in output.split("\n"):
                    if "htc" in self.run_on:
                        if "cluster" in line:
                            cluster_id = int(line.split("cluster ")[1][:-1])
                            dic_id_to_job_temp[cluster_id] = l_jobs[idx_submission]
                            idx_submission += 1
                    elif "slurm" in self.run_on:
                        if "Submitted" in line:
                            job_id = int(line.split(" ")[3])
                            dic_id_to_job_temp[job_id] = l_jobs[idx_submission]
                            idx_submission += 1
        # Update and write the id-job file
        if dic_id_to_job_temp:
            assert len(dic_id_to_job_temp) == len(l_jobs)

        # Merge with the previous id-job file
        dic_id_to_job = self.dic_id_to_job

        # Update and write on disk
        if dic_id_to_job is not None:
            dic_id_to_job.update(dic_id_to_job_temp)
            self.dic_id_to_job = dic_id_to_job
        elif dic_id_to_job_temp:
            dic_id_to_job = dic_id_to_job_temp
            self.dic_id_to_job = dic_id_to_job

        print("Jobs status after submission:")
        running_jobs, queuing_jobs = self._get_state_jobs(verbose=True)

    def _get_local_jobs(self):
        l_jobs = []
        # Warning, does not work at the moment in lxplus...
        for ps in psutil.pids():
            try:
                aux = psutil.Process(ps).cmdline()
            except Exception:
                aux = []
            if len(aux) > 1 and "run.sh" in aux[-1]:
                job = str(Path(aux[-1]).parent)

                # Only get path after name of the study
                job = job.split(self.study_name)[1]

                l_jobs.append(f"{job}/")
        return l_jobs

    @staticmethod
    def _get_condor_jobs(status, dic_id_to_job=None, force_query_individually=False):
        l_jobs = []
        dic_status = {"running": 1, "queuing": 2}
        condor_output = subprocess.run(["condor_q"], capture_output=True).stdout.decode("utf-8")

        # Check which jobs are running
        first_line = True
        first_missing_job = True
        for line in condor_output.split("\n")[4:]:
            if line == "":
                break
            jobid = int(line.split("ID:")[1][1:8])
            condor_status = line.split("      ")[1:5]  # Done, Run, Idle, and potentially Hold

            # If job is running/queuing, get the path to the job
            if condor_status[dic_status[status]] == "1":
                # Get path from dic_id_to_job if available
                if dic_id_to_job is not None:
                    if jobid in dic_id_to_job:
                        l_jobs.append(dic_id_to_job[jobid])
                    elif first_missing_job:
                        print(
                            "Warning, some jobs are queuing/running and are not in the id-job"
                            " file. They may come from another study. Ignoring them."
                        )
                        first_missing_job = False

                elif force_query_individually:
                    if first_line:
                        print(
                            "Warning, some jobs are queuing/running and the id-job file is"
                            " missing... Querying them individually."
                        )
                        first_line = False
                    job_details = subprocess.run(
                        ["condor_q", "-l", f"{jobid}"], capture_output=True
                    ).stdout.decode("utf-8")
                    job = job_details.split('Cmd = "')[1].split("run.sh")[0]

                    # Only get path after master_study
                    job = job.split("master_study")[1]
                    l_jobs.append(job)

                elif first_line:
                    print(
                        "Warning, some jobs are queuing/running and the id-job file is"
                        " missing... Ignoring them."
                    )
                    first_line = False

        return l_jobs

    @staticmethod
    def _get_slurm_jobs(status, dic_id_to_job=None, force_query_individually=False):
        l_jobs = []
        dic_status = {"running": "RUNNING", "queuing": "PENDING"}
        username = (
            subprocess.run(["id", "-u", "-n"], capture_output=True).stdout.decode("utf-8").strip()
        )
        slurm_output = subprocess.run(
            ["squeue", "-u", f"{username}", "-t", dic_status[status]], capture_output=True
        ).stdout.decode("utf-8")

        # Get job id and details
        first_line = True
        first_missing_job = True
        for line in slurm_output.split("\n")[1:]:
            l_split = line.split()
            if len(l_split) == 0:
                break
            jobid = int(l_split[0])
            slurm_status = l_split[4]  # R or PD  # noqa: F841

            # Get path from dic_id_to_job if available
            if dic_id_to_job is not None:
                if jobid in dic_id_to_job:
                    l_jobs.append(dic_id_to_job[jobid])
                elif first_missing_job:
                    print(
                        "Warning, some jobs are queuing/running and are not in the id-job"
                        " file. They may come from another study. Ignoring them."
                    )
                    first_missing_job = False

            elif force_query_individually:
                if first_line:
                    print(
                        "Warning, some jobs are queuing/running and the id-job file is"
                        " missing... Querying them individually."
                    )
                    first_line = False
                job_details = subprocess.run(
                    ["scontrol", "show", "jobid", "-dd", f"{jobid}"], capture_output=True
                ).stdout.decode("utf-8")
                job = (
                    job_details.split("Command=")[1].split("run.sh")[0]
                    if "run.sh" in job_details
                    else job_details.split("StdOut=")[1].split("output.txt")[0]
                )
                # Only get path after master_study
                job = job.split("master_study")[1]
                l_jobs.append(job)

            elif first_line:
                print(
                    "Warning, some jobs are queuing/running and the id-job file is"
                    " missing... Ignoring them."
                )
                first_line = False

        return l_jobs

    def querying_jobs(self, status="running"):
        # sourcery skip: remove-redundant-if, remove-redundant-pass, swap-nested-ifs
        # Find out which type of submission to query
        set_submission_type = set()
        for job in self.l_jobs_to_submit:
            l_keys = self.dic_all_jobs[job]["l_keys"]
            set_submission_type.add(nested_get(self.dic_tree, l_keys + ["submission_type"]))

        # Ensure that submission does not mix different htc and slurm
        if ("htc" in set_submission_type or "htc_docker" in set_submission_type) and (
            "slurm" in set_submission_type or "slurm_docker" in set_submission_type
        ):
            raise ValueError("Error: Mixing htc and slurm submission is not allowed")

        l_jobs = []
        if "local_pc" in set_submission_type:
            if status == "running":
                l_jobs.extend(self._get_local_jobs())
            else:
                # Always empty return as there is no queuing in local pc
                pass

        if set_submission_type.intersection(["htc", "htc_docker"]) != set():
            l_jobs.extend(self._get_condor_jobs(status, self.dic_id_to_job))

        if set_submission_type.intersection(["slurm", "slurm_docker"]) != set():
            l_jobs.extend(self._get_slurm_jobs(status, self.dic_id_to_job))

        return l_jobs
