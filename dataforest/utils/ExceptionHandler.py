import traceback


class ExceptionHandler:
    @staticmethod
    def handle(branch, e, logfile_name, stop):
        try:
            ExceptionHandler._log_error(branch, e, logfile_name)
        finally:
            if stop:
                raise e

    @staticmethod
    def _log_error(branch, e, logfile_name):
        log_dir = branch[branch.current_process].logs_path
        log_dir.mkdir(exist_ok=True)
        log_path = log_dir / logfile_name
        with open(log_path, "w") as f:
            f.write("Traceback (most recent call last):")
            traceback.print_tb(e.__traceback__, file=f)
            f.write(f"\n{str(e)}")
