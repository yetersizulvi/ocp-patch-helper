import logging
import unittest
from app.central_logging import HealthAccessFilter, configure

class HealthLoggingTests(unittest.TestCase):
    def test_successful_probes_only_are_suppressed(self):
        filter_=HealthAccessFilter()
        for path,status,expected in [('/health/live',200,False),('/health/ready?probe=1',200,False),('/health/ready',503,True),('/health/live',405,True),('/api/v1/sessions',200,True),('/health/live/other',200,True)]:
            record=logging.LogRecord('uvicorn.access',logging.INFO,'',0,'%s - "%s %s HTTP/%s" %d',('client','GET',path,'1.1',status),None)
            self.assertEqual(expected,filter_.filter(record),(path,status))
        record=logging.LogRecord('uvicorn.access',logging.INFO,'',0,'unstructured line',(),None)
        self.assertTrue(filter_.filter(record))

    def test_configuration_does_not_stack_access_filters(self):
        configure();configure()
        self.assertEqual(1,sum(isinstance(f,HealthAccessFilter) for f in logging.getLogger('uvicorn.access').filters))
