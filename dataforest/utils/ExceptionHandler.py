import logging
import traceback


class ExceptionHandler:
    _LOG = logging.getLogger("ExceptionHandler")

    @classmethod
    def handle(cls, branch, e, logfile_name, stop):
        try:
            log_path = cls._get_log_path(branch, logfile_name)
        except Exception as path_e:
            cls._LOG.warning(f"{type(path_e).__name__} encountered getting logging output path for {logfile_name}")
            if stop:
                raise e
        else:
            cls._handle_write(e, log_path, stop)

    @classmethod
    def _handle_write(cls, e, log_path, stop):
        try:
            cls._write_log(e, log_path)
            cls._LOG.info(f"Wrote log to {log_path}")
        except Exception as write_e:
            cls._LOG.warning(f"{type(write_e).__name__} encountered writing {log_path}")
        finally:
            if stop:
                raise e
            else:
                cls._LOG.warning(f"{type(e).__name__} encountered but `stop_on_error=False`. Logs at {log_path}")

    @staticmethod
    def _get_log_path(branch, logfile_name):
        log_dir = branch[branch.current_process].logs_path
        log_dir.mkdir(exist_ok=True)
        return log_dir / logfile_name

    @staticmethod
    def _write_log(e, log_path):
        with open(log_path, "w") as f:
            f.write("Traceback (most recent call last):")
            traceback.print_tb(e.__traceback__, file=f)
            f.write(f"\n{str(e)}")
