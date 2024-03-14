# ==================================================================================================
# --- Imports
# ==================================================================================================
# Standard library imports

# ==================================================================================================
# --- Class for job submission
# ==================================================================================================
class SubmissionStatement:
    def __init__(self, path_job_folder, sub_filename):
        self.path_job_folder = path_job_folder
        self.sub_filename = sub_filename


class LocalPC(SubmissionStatement):
    def __init__(self, path_job_folder, sub_filename):
        super().__init__(path_job_folder, sub_filename)

        self.head = "# Running on local pc"
        self.body = f"bash {path_job_folder}/run.sh &"
        self.tail = "# Local pc"
        self.submit_command = f"bash {sub_filename}"


class Slurm(SubmissionStatement):
    def __init__(self, path_job_folder, sub_filename, slurm_queue_statement, request_GPUs):
        super().__init__(path_job_folder, sub_filename)

        self.head = "# Running on SLURM "
        self.body = (
            f"sbatch --ntasks=2 {slurm_queue_statement.split(' ')[1] if slurm_queue_statement != "" else slurm_queue_statement}"
            "--output=output.txt --error=error.txt --gres=gpu:{request_GPUs} {path_job_folder}/run.sh"
        )
        self.tail = "# SLURM"
        self.submit_command = f"bash {sub_filename}"


class SlurmDocker(SubmissionStatement):
    def __init__(
        self, path_job_folder, sub_filename, slurm_queue_statement, request_GPUs, path_image
    ):
        super().__init__(path_job_folder, sub_filename)

        self.head = (
            "#!/bin/bash\n"
            + "# This is a SLURM submission file using Docker\n"
            + slurm_queue_statement
            + "\n"
            + f"#SBATCH --output={path_job_folder}/output.txt\n"
            + f"#SBATCH --error={path_job_folder}/error.txt\n"
            + "#SBATCH --ntasks=2\n"
            + f"#SBATCH --gres=gpu:{request_GPUs}"
        )
        self.body = f"singularity exec {path_image} {path_job_folder}/run.sh"
        self.tail = "# SLURM Docker"
        self.submit_command = f"sbatch {sub_filename}"


class HTC(SubmissionStatement):
    def __init__(self, path_job_folder, sub_filename, request_GPUs):
        super().__init__(path_job_folder, sub_filename)

        self.head = (
            "# This is a HTCondor submission file\n"
            + "error  = error.txt\n"
            + "output = output.txt\n"
            + "log  = log.txt"
        )
        self.body = (
            f"initialdir = {path_job_folder}\n"
            + f"executable = {path_job_folder}/run.sh\n"
            + f"request_GPUs = {request_GPUs}\n"
            + "queue"
        )
        self.tail = "# HTC"
        self.submit_command = f"condor_submit {sub_filename}"


class HTCDocker(SubmissionStatement):
    def __init__(self, path_job_folder, sub_filename, request_GPUs, path_image):
        super().__init__(path_job_folder, sub_filename)

        self.head = (
            "# This is a HTCondor submission file using Docker\n"
            + "error  = error.txt\n"
            + "output = output.txt\n"
            + "log  = log.txt\n"
            + "universe = vanilla\n"
            + "+SingularityImage ="
            + f' "{path_image}"'
        )
        self.body = (
            f"initialdir = {path_job_folder}\n"
            + f"executable = {path_job_folder}/run.sh\n"
            + f"request_GPUs = {request_GPUs}\n"
            + "queue"
        )
        self.tail = "# HTC Docker"
        self.submit_command = f"condor_submit {sub_filename}"
