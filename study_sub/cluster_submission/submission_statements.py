# ==================================================================================================
# --- Imports
# ==================================================================================================
# Standard library imports


# ==================================================================================================
# --- Class for job submission
# ==================================================================================================
class SubmissionStatement:
    def __init__(self, path_node, filename):
        self.path_node = path_node
        self.filename = filename

        self.head = ""
        self.body = ""
        self.tail = ""
        self.submit_command = ""

    def write(self):
        with open(self.filename, "w") as file:
            file.write(self.head)
            file.write(self.body)
            file.write(self.tail)
            file.write(self.submit_command)
            file.write("\n")


class LocalPC(SubmissionStatement):
    def __init__(self, path_node, filename):
        super().__init__(path_node, filename)

        self.head = "# Running on local pc"
        self.body = f"bash {path_node}/run.sh &"
        self.tail = "# Local pc"
        self.submit_command = f"bash {filename}"


class Slurm(SubmissionStatement):
    def __init__(self, path_node, filename, slurm_queue_statement, request_GPUs):
        super().__init__(path_node, filename)

        self.head = "# Running on SLURM "
        self.body = (
            f"sbatch --ntasks=2 {slurm_queue_statement.split(' ')[1] if slurm_queue_statement != "" else slurm_queue_statement}"
            "--output=output.txt --error=error.txt --gres=gpu:{request_GPUs} {path_node}/run.sh"
        )
        self.tail = "# SLURM"
        self.submit_command = f"bash {filename}"


class SlurmDocker(SubmissionStatement):
    def __init__(self, path_node, filename, slurm_queue_statement, request_GPUs, path_image):
        super().__init__(path_node, filename)

        self.head = (
            "#!/bin/bash\n"
            + "# This is a SLURM submission file using Docker\n"
            + slurm_queue_statement
            + "\n"
            + f"#SBATCH --output={path_node}/output.txt\n"
            + f"#SBATCH --error={path_node}/error.txt\n"
            + "#SBATCH --ntasks=2\n"
            + f"#SBATCH --gres=gpu:{request_GPUs}"
        )
        self.body = f"singularity exec {path_image} {path_node}/run.sh"
        self.tail = "# SLURM Docker"
        self.submit_command = f"sbatch {filename}"


class HTC(SubmissionStatement):
    def __init__(self, path_node, filename, request_GPUs):
        super().__init__(path_node, filename)

        self.head = (
            "# This is a HTCondor submission file\n"
            + "error  = error.txt\n"
            + "output = output.txt\n"
            + "log  = log.txt"
        )
        self.body = (
            f"initialdir = {path_node}\n"
            + f"executable = {path_node}/run.sh\n"
            + f"request_GPUs = {request_GPUs}\n"
            + "queue"
        )
        self.tail = "# HTC"
        self.submit_command = f"condor_submit {filename}"


class HTCDocker(SubmissionStatement):
    def __init__(self, path_node, filename, request_GPUs, path_image):
        super().__init__(path_node, filename)

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
            f"initialdir = {path_node}\n"
            + f"executable = {path_node}/run.sh\n"
            + f"request_GPUs = {request_GPUs}\n"
            + "queue"
        )
        self.tail = "# HTC Docker"
        self.submit_command = f"condor_submit {filename}"
