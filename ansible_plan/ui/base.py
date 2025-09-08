import threading
import os
import logging
import logging.handlers
import abc
from .api_client import ApiClient
from ..core.models import WorkflowStatus

class WorkflowOutput(threading.Thread):
    '''
    A general workflow output class to be implemented by subclasses
    '''
    def __init__(self, backend_url, event, logging_dir, log_level, cmd_args):
        threading.Thread.__init__(self)
        self._define_logger(logging_dir, log_level)
        self.api_client = ApiClient(backend_url, logger=self._logger)
        self._refresh_interval = 2
        self.__verify_only = cmd_args.verify_only
        self.__interactive_retry = cmd_args.interactive_retry
        self.event: threading.Event = event

    def is_verify_only(self):
        return self.__verify_only

    def _define_logger(self, logging_dir, level):
        logger_name = self.__class__.__name__
        # Use a fixed log name for now, as we don't have the workflow object here.
        self._log_name = "frontend.log"
        logger_file_path = os.path.join(logging_dir, self._log_name)
        if not os.path.exists(os.path.dirname(logger_file_path)):
            os.makedirs(os.path.dirname(logger_file_path))

        logger = logging.getLogger(logger_name)
        if level:
            logger.setLevel(getattr(logging, level.upper()))
        logger_handler = logging.handlers.TimedRotatingFileHandler(
            logger_file_path,
            when='d',
            backupCount=3,
            encoding='utf8'
        )
        logger_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
        logger.addHandler(logger_handler)
        self._logger = logger
        self._logging_dir = logging_dir

    def run(self):
        self._logger.info("WorkflowOutput run")
        self.draw_init()

        status_data = None
        while not self.event.is_set():
            status_data = self.api_client.get_workflow_status()
            status = status_data.get('status') if status_data else None
            self._logger.info(f"Checking status: {status}")

            if status == WorkflowStatus.ENDED.value:
                break

            if status == WorkflowStatus.FAILED.value and not self.__interactive_retry:
                break

            self.draw_step()

            if hasattr(self, 'user_chose_to_quit') and self.user_chose_to_quit:
                break

            self.draw_pause()

        if not self.event.is_set():
            self._logger.info(f"Final status: {status}. Exiting loop.")
            self.draw_end(status_data=status_data)

    @abc.abstractmethod
    def draw_init(self, *args, **kwargs):
        ''' Draw initialization'''
        pass

    @abc.abstractmethod
    def draw_end(self, status_data: dict = None, *args, **kwargs):
        ''' Draw initialization'''
        pass

    @abc.abstractmethod
    def draw_step(self):
        ''' Draw the workflow'''
        pass

    @abc.abstractmethod
    def draw_pause(self):
        ''' Need to be implemented to pause after a draw step'''
        pass
