# ==================================================================================================
# --- Imports
# ==================================================================================================
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
        abs_path_study: str,
    ):
        self.study_name = study_name
        self.l_jobs_to_submit = l_jobs_to_submit
        self.dic_all_jobs = dic_all_jobs
        self.dic_tree = dic_tree
        self.path_submission_file = path_submission_file
        self.abs_path_study = abs_path_study
        self.dic_submission = {
            "local": LocalPC,
            "htc": HTC,
            "htc_docker": HTCDocker,
            "slurm": Slurm,
            "slurm_docker": SlurmDocker,
        }

    # Getter for dic_id_to_path_job
    @property
    def dic_id_to_path_job(self):
        dic_id_to_path_job = {}
        found_at_least_one = False
        for job in self.l_jobs_to_submit:
            l_keys = self.dic_all_jobs[job]["l_keys"]
            subdic_job = nested_get(self.dic_tree, l_keys)
            if "id_sub" in subdic_job:
                dic_id_to_path_job[subdic_job["id_sub"]] = self._return_abs_path_job(job)[0]
                found_at_least_one = True

        return dic_id_to_path_job if found_at_least_one else None

    # Setter for dic_id_to_path_job
    @dic_id_to_path_job.setter
    def dic_id_to_path_job(self, dic_id_to_path_job):
        assert isinstance(dic_id_to_path_job, dict)
        # Ensure all ids are integers
        dic_id_to_path_job = {
            int(id_job): path_job for id_job, path_job in dic_id_to_path_job.items()
        }
        dic_job_to_id = {path_job: int(id_job) for id_job, path_job in dic_id_to_path_job.items()}

        # Update the tree
        for job in self.l_jobs_to_submit:
            path_job = self._return_abs_path_job(job)[0]
            l_keys = self.dic_all_jobs[job]["l_keys"]
            subdic_job = nested_get(self.dic_tree, l_keys)
            if "id_sub" in subdic_job and int(subdic_job["id_sub"]) not in dic_id_to_path_job:
                del subdic_job["id_sub"]
            elif "id_sub" not in subdic_job and path_job in dic_job_to_id:
                subdic_job["id_sub"] = dic_job_to_id[path_job]
            # Else all is consistent

    def _update_dic_id_to_path_job(self, running_jobs, queuing_jobs):
        # Look for jobs in the dictionnary that are not running or queuing anymore
        set_current_jobs = set(running_jobs + queuing_jobs)
        if self.dic_id_to_path_job is not None:
            dic_id_to_path_job = self.dic_id_to_path_job
            for id_job, job in self.dic_id_to_path_job.items():
                if job not in set_current_jobs:
                    del dic_id_to_path_job[id_job]

            # Update dic_id_to_path_job
            self.dic_id_to_path_job = dic_id_to_path_job

    def _check_submission_type(self):
        check_local = False
        check_htc = False
        check_slurm = False
        for job in self.l_jobs_to_submit:
            l_keys = self.dic_all_jobs[job]["l_keys"]
            submission_type = nested_get(self.dic_tree, l_keys + ["submission_type"])
            if submission_type == "local":
                check_local = True
            elif submission_type in ["htc", "htc_docker"]:
                check_htc = True
            elif submission_type in ["slurm", "slurm_docker"]:
                check_slurm = True

        if check_htc and check_slurm:
            raise ValueError("Error: Mixing htc and slurm submission is not allowed")

        return check_local, check_htc, check_slurm

    def _get_state_jobs(self, verbose=True):
        # First check whether the jobs are submitted on local, htc or slurm
        check_local, check_htc, check_slurm = self._check_submission_type()

        # Then query accordingly
        running_jobs = self.querying_jobs(check_local, check_htc, check_slurm, status="running")
        queuing_jobs = self.querying_jobs(check_local, check_htc, check_slurm, status="queuing")
        self._update_dic_id_to_path_job(running_jobs, queuing_jobs)
        if verbose:
            print("Running: \n" + "\n".join(running_jobs))
            print("queuing: \n" + "\n".join(queuing_jobs))
        return running_jobs, queuing_jobs

    def _test_job(self, job, path_job, running_jobs, queuing_jobs):
        # Test if job is completed
        l_keys = self.dic_all_jobs[job]["l_keys"]
        completed = nested_get(self.dic_tree, l_keys + ["status"]) == "finished"
        if completed:
            print(f"{path_job} is already completed.")

        # Test if job is running
        elif path_job in running_jobs:
            print(f"{path_job} is already running.")

        # Test if job is queuing
        elif path_job in queuing_jobs:
            print(f"{path_job} is already queuing.")

        # True if job must be (re)submitted
        else:
            return True
        return False

    def _return_htc_flavour(self, job):
        l_keys = self.dic_all_jobs[job]["l_keys"]
        return nested_get(self.dic_tree, l_keys + ["htc_flavor"])

    def _return_abs_path_job(self, job):
        # Get corresponding path job (remove the python file name)
        path_job = "/".join(job.split("/")[:-1]) + "/"
        abs_path_job = f"{self.abs_path_study}/{path_job}"
        return path_job, abs_path_job

    def _write_sub_files_slurm_docker(self, sub_filename, running_jobs, queuing_jobs, list_of_jobs):
        l_filenames = []
        list_of_jobs_updated = []
        for idx_job, job in enumerate(list_of_jobs):
            path_job, abs_path_job = self._return_abs_path_job(job)

            # Test if job is running, queuing or completed
            if self._test_job(job, path_job, running_jobs, queuing_jobs):
                filename_sub = f"{sub_filename.split('.sub')[0]}_{idx_job}.sub"

                # Get job context
                l_keys = self.dic_all_jobs[job]["l_keys"]
                context = nested_get(self.dic_tree, l_keys + ["context"])

                # Write the submission files
                # ! Careful, I implemented a fix for path due to the temporary home recovery folder
                print(f'Writing submission file for node "{abs_path_job}"')
                fix = True
                Sub = self.dic_submission["slurm_docker"](
                    filename_sub, abs_path_job, context, self.dic_tree["container_image"], fix=fix
                )
                with open(filename_sub, "w") as fid:
                    fid.write(Sub.head + "\n")
                    if fix:
                        fid.write(Sub.str_fixed_run + "\n")
                    fid.write(Sub.body + "\n")
                    fid.write(Sub.tail + "\n")

                l_filenames.append(filename_sub)
                list_of_jobs_updated.append(job)
        return l_filenames, list_of_jobs_updated

    def _get_Sub(self, job, submission_type, sub_filename, abs_path_job, context):
        match submission_type:
            case "slurm":
                return self.dic_submission[submission_type](sub_filename, abs_path_job, context)
            case "htc":
                return self.dic_submission[submission_type](
                    sub_filename, abs_path_job, context, self._return_htc_flavour(job)
                )
            case w if w in ["htc_docker", "slurm_docker"]:
                # Path to singularity image
                if (
                    "container_image" in self.dic_tree
                    and self.dic_tree["container_image"] is not None
                ):
                    self.path_image = self.dic_tree["container_image"]
                else:
                    raise ValueError(
                        "Error: container_image is not defined in the tree. Please define it in the"
                        " config.yaml file."
                    )

                if submission_type == "htc_docker":
                    return self.dic_submission[submission_type](
                        sub_filename,
                        abs_path_job,
                        context,
                        self.path_image,
                        self._return_htc_flavour(job),
                    )
                else:
                    return self.dic_submission[submission_type](
                        sub_filename, abs_path_job, context, self.path_image
                    )
            case "local":
                return self.dic_submission[submission_type](sub_filename, abs_path_job)
            case _:
                raise ValueError(f"Error: {submission_type} is not a valid submission mode")

    def _write_sub_file(
        self,
        sub_filename,
        running_jobs,
        queuing_jobs,
        list_of_jobs,
        submission_type,
    ):
        # Flag to know if the file can be submitted (at least one job in it)
        ok_to_submit = False

        # Flat to know if the header has been written
        header_written = False

        # Create folder to the submission file if it does not exist
        os.makedirs("/".join(sub_filename.split("/")[:-1]), exist_ok=True)

        # Updated list of jobs (without unsubmitted jobs)
        list_of_jobs_updated = []

        # Write the submission file
        Sub = None
        with open(sub_filename, "w") as fid:
            for job in list_of_jobs:
                # Get corresponding path job (remove the python file name)
                path_job, abs_path_job = self._return_abs_path_job(job)

                # Test if job is running, queuing or completed
                if self._test_job(job, path_job, running_jobs, queuing_jobs):
                    print(f'Writing submission command for node "{abs_path_job}"')

                    # Get context
                    l_keys = self.dic_all_jobs[job]["l_keys"]
                    context = nested_get(self.dic_tree, l_keys + ["context"])

                    # Get Submission object
                    Sub = self._get_Sub(job, submission_type, sub_filename, abs_path_job, context)

                    # Take the first job as reference for head
                    if not header_written:
                        fid.write(Sub.head + "\n")
                        header_written = True

                    # Write instruction for submission
                    fid.write(Sub.body + "\n")

                    # Append job to list_of_jobs_updated
                    list_of_jobs_updated.append(job)

            # Tail instruction
            if Sub is not None:
                fid.write(Sub.tail + "\n")
                ok_to_submit = True

        if not ok_to_submit:
            os.remove(sub_filename)

        return ([sub_filename], list_of_jobs_updated) if ok_to_submit else ([], [])

    def _write_sub_files(
        self,
        sub_filename,
        running_jobs,
        queuing_jobs,
        list_of_jobs,
        submission_type,
    ):
        # Slurm docker is a peculiar case as one submission file must be created per job
        if submission_type == "slurm_docker":
            return self._write_sub_files_slurm_docker(
                sub_filename, running_jobs, queuing_jobs, list_of_jobs
            )

        else:
            return self._write_sub_file(
                sub_filename,
                running_jobs,
                queuing_jobs,
                list_of_jobs,
                submission_type,
            )

    def write_sub_files(self):
        running_jobs, queuing_jobs = self._get_state_jobs(verbose=False)

        # Make a dict of all jobs to submit depending on the submission type
        dic_jobs_to_submit = {key: [] for key in self.dic_submission.keys()}
        for job in self.l_jobs_to_submit:
            l_keys = self.dic_all_jobs[job]["l_keys"]
            submission_type = nested_get(self.dic_tree, l_keys + ["submission_type"])
            dic_jobs_to_submit[submission_type].append(job)  # type: ignore

        # Write submission files for each submission type
        dic_submission_files = {}
        for submission_type, list_of_jobs in dic_jobs_to_submit.items():
            if len(list_of_jobs) > 0:
                # Write submission files
                l_submission_filenames, list_of_jobs_updated = self._write_sub_files(
                    self.path_submission_file,
                    running_jobs,
                    queuing_jobs,
                    list_of_jobs,
                    submission_type,
                )

                # Record submission files and context
                dic_submission_files[submission_type] = (
                    list_of_jobs_updated,
                    l_submission_filenames,
                )

        return dic_submission_files

    def _update_job_status_from_hpc_output(
        self,
        submit_command,
        submission_type,
        dic_id_to_path_job_temp,
        list_of_jobs,
        idx_submission=0,
    ):
        process = subprocess.run(
            submit_command.split(" "),
            capture_output=True,
        )

        output = process.stdout.decode("utf-8")
        output_error = process.stderr.decode("utf-8")
        if "ERROR" in output_error:
            raise RuntimeError(f"Error in submission: {output}")
        for line in output.split("\n"):
            if "htc" in submission_type:
                if "cluster" in line:
                    cluster_id = int(line.split("cluster ")[1][:-1])
                    dic_id_to_path_job_temp[cluster_id] = self._return_abs_path_job(
                        list_of_jobs[idx_submission]
                    )[0]
                    idx_submission += 1
            elif "slurm" in submission_type:
                if "Submitted" in line:
                    job_id = int(line.split(" ")[3])
                    dic_id_to_path_job_temp[job_id] = self._return_abs_path_job(
                        list_of_jobs[idx_submission]
                    )[0]
                    idx_submission += 1

        return dic_id_to_path_job_temp, idx_submission

    def submit(self, list_of_jobs, l_submission_filenames, submission_type):
        # Check that the submission file(s) is/are appropriate for the submission mode
        if len(l_submission_filenames) > 1 and submission_type != "slurm_docker":
            raise ValueError(
                "Error: Multiple submission files should not be implemented for this submission"
                " mode"
            )

        # Check that at least one job is being submitted
        if len(l_submission_filenames) == 0:
            print("No job being submitted.")

        # Submit
        dic_id_to_path_job_temp = {}
        idx_submission = 0
        for sub_filename in l_submission_filenames:
            if submission_type == "local":
                os.system(self.dic_submission[submission_type].get_submit_command(sub_filename))
            elif submission_type in ["htc", "slurm", "htc_docker", "slurm_docker"]:
                submit_command = self.dic_submission[submission_type].get_submit_command(
                    sub_filename
                )
                dic_id_to_path_job_temp, idx_submission = self._update_job_status_from_hpc_output(
                    submit_command,
                    submission_type,
                    dic_id_to_path_job_temp,
                    list_of_jobs,
                    idx_submission,
                )
            else:
                raise ValueError(f"Error: {submission_type} is not a valid submission mode")

        # Update and write the id-job file
        if dic_id_to_path_job_temp:
            assert len(dic_id_to_path_job_temp) == len(list_of_jobs)

        # Merge with the previous id-job file
        dic_id_to_path_job = self.dic_id_to_path_job

        # Update and write on disk
        if dic_id_to_path_job is not None:
            dic_id_to_path_job.update(dic_id_to_path_job_temp)
            self.dic_id_to_path_job = dic_id_to_path_job
        elif dic_id_to_path_job_temp:
            dic_id_to_path_job = dic_id_to_path_job_temp
            self.dic_id_to_path_job = dic_id_to_path_job

        print("Jobs status after submission:")
        running_jobs, queuing_jobs = self._get_state_jobs(verbose=True)

    def _get_local_jobs(self):
        l_path_jobs = []
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
                l_path_jobs.append(f"{self.study_name}{job}/")
        return l_path_jobs

    def _get_condor_jobs(self, status, force_query_individually=False):
        l_path_jobs = []
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
                # Get path from dic_id_to_path_job if available
                if self.dic_id_to_path_job is not None:
                    if jobid in self.dic_id_to_path_job:
                        l_path_jobs.append(self.dic_id_to_path_job[jobid])
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
                    job = job.split(self.study_name)[1]
                    l_path_jobs.append(f"{self.study_name}{job}")

                elif first_line:
                    print(
                        "Warning, some jobs are queuing/running and the id-job file is"
                        " missing... Ignoring them."
                    )
                    first_line = False

        return l_path_jobs

    def _get_slurm_jobs(self, status, force_query_individually=False):
        l_path_jobs = []
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

            # Get path from dic_id_to_path_job if available
            if self.dic_id_to_path_job is not None:
                if jobid in self.dic_id_to_path_job:
                    l_path_jobs.append(self.dic_id_to_path_job[jobid])
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
                # Only get path after study_name
                job = job.split(self.study_name)[1]
                l_path_jobs.append(f"{self.study_name}{job}")

            elif first_line:
                print(
                    "Warning, some jobs are queuing/running and the id-job file is"
                    " missing... Ignoring them."
                )
                first_line = False

        return l_path_jobs

    def querying_jobs(
        self, check_local: bool, check_htc: bool, check_slurm: bool, status="running"
    ):
        # sourcery skip: remove-redundant-if, remove-redundant-pass, swap-nested-ifs
        l_path_jobs = []
        if check_local:
            if status == "running":
                l_path_jobs.extend(self._get_local_jobs())
            else:
                # Always empty return as there is no queuing in local pc
                pass

        if check_htc:
            l_path_jobs.extend(self._get_condor_jobs(status))

        if check_slurm:
            l_path_jobs.extend(self._get_slurm_jobs(status))

        return l_path_jobs
