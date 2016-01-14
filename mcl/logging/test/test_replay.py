import os
import time
import unittest
import multiprocessing
import mcl.message.messages
from mcl.logging.replay import BufferData
from mcl.logging.file import ReadDirectory
from mcl.network.network import MessageListener
from mcl.logging.replay import ScheduleBroadcasts
from mcl.network.udp import Connection as Connection

TIMEOUT = 1.0
_DIRNAME = os.path.dirname(__file__)
LOG_PATH = os.path.join(_DIRNAME, 'dataset')


# -----------------------------------------------------------------------------
#                           Objects for unit-testing
# -----------------------------------------------------------------------------
URL_A = 'ff15::c75d:ce41:ea8e:000a'
URL_B = 'ff15::c75d:ce41:ea8e:000b'

# WARNING: this should not be deployed in production code. It is an
#          abuse that has been used for the purposes of unit-testing.
mcl.message.messages._MESSAGES = list()


class UnitTestMessageA(mcl.message.messages.Message):
    mandatory = ('data',)
    connection = Connection(URL_A)


class UnitTestMessageB(mcl.message.messages.Message):
    mandatory = ('data',)
    connection = Connection(URL_B)


# -----------------------------------------------------------------------------
#                                 BufferData()
# -----------------------------------------------------------------------------
class TestBufferData(unittest.TestCase):

    def test_init(self):
        """Test BufferData() instantiation."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        BufferData(reader)

        # Initialise buffer object with queue length.
        buf = BufferData(reader, length=100)
        self.assertEqual(buf.length, 100)
        with self.assertRaises(TypeError):
            BufferData(reader, length='100')

    def test_start_stop(self):
        """Test BufferData() start/stop."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader)
        self.assertFalse(buf.is_alive())

        # Start buffering data.
        self.assertTrue(buf.start())
        self.assertTrue(buf.is_alive())
        self.assertFalse(buf.start())

        # Stop buffering data.
        self.assertTrue(buf.stop())
        self.assertFalse(buf.is_alive())
        self.assertFalse(buf.stop())

        # Allow threads to fully shut down.
        time.sleep(0.1)

    def test_buffer(self):
        """Test BufferData() buffering functionality."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader)

        # Check capacity of buffer/queue.
        self.assertFalse(buf.is_ready())
        self.assertTrue(buf.is_data_pending)
        self.assertTrue(buf.queue.empty())
        self.assertFalse(buf.queue.full())

        # Start buffering data and wait until files have been completely read.
        buf.start()
        start_wait = time.time()
        while buf.is_alive() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Check capacity of buffer/queue.
        self.assertTrue(buf.is_ready())
        self.assertFalse(buf.is_data_pending())
        self.assertFalse(buf.queue.empty())
        self.assertEqual(buf.queue.qsize(), 20)

        # Read data from queue.
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Read data from queue.
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(10 * message['elapsed_time']), i + 1)

    def test_read_blocks(self):
        """Test BufferData() can read data in blocks and start/stop."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader, length=10)
        self.assertFalse(buf.is_ready())

        # Start buffering data and wait until buffer is full.
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Read data from queue.
        #
        # Check capacity of buffer/queue and read all data from the queue. Note
        # that BufferData() is still buffering and aught to buffer the
        # remaining data to the queue while the first few elements are being
        # checked here.
        #
        self.assertTrue(buf.is_ready())
        self.assertEqual(buf.queue.qsize(), 10)
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Wait until remaining data has been read.
        start_wait = time.time()
        while buf.is_alive() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Read remaining data from queue.
        for i in range(1, 11):
            message = buf.queue.get()
            self.assertEqual(int(10 * message['elapsed_time']), i)

        # Check capacity of buffer.
        self.assertTrue(buf.is_ready())
        self.assertFalse(buf.is_data_pending())
        self.assertEqual(buf.queue.qsize(), 0)

    def test_reset(self):
        """Test BufferData() can be reset."""

        # Create data reader object.
        reader = ReadDirectory(LOG_PATH, message=True)

        # Initialise buffer object.
        buf = BufferData(reader, length=10)

        # Start buffering data and wait until buffer is full.
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Read data from queue.
        #
        # Check capacity of buffer/queue and read all data from the queue. Note
        # that BufferData() is still buffering and aught to buffer the
        # remaining data to the queue while the first few elements are being
        # checked here.
        buf.stop()
        self.assertEqual(buf.queue.qsize(), 10)
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)

        # Reset buffer and start buffering data from the beginning of the file
        # again.
        buf.reset()
        buf.start()
        start_wait = time.time()
        while not buf.queue.full() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        buf.stop()
        self.assertEqual(buf.queue.qsize(), 10)
        for i in range(0, 10):
            message = buf.queue.get()
            self.assertEqual(int(100 * message['elapsed_time']), i)


# -----------------------------------------------------------------------------
#                             ScheduleBroadcasts()
# -----------------------------------------------------------------------------
class TestScheduleBroadcasts(unittest.TestCase):

    def test_init(self):
        """Test ScheduleBroadcasts() default instantiation."""

        # Create multiprocessing queue.
        queue = multiprocessing.Queue()
        scheduler = ScheduleBroadcasts(queue)

        # Initialise scheduler and ensure the first input is a multiprocessing
        # queue.
        self.assertEqual(scheduler.queue, queue)
        with self.assertRaises(TypeError):
            ScheduleBroadcasts('queue')

    def test_init_speed(self):
        """Test ScheduleBroadcasts() speed instantiation."""

        # Initialise scheduler object with a speed argument.
        speed = 3
        queue = multiprocessing.Queue()
        scheduler = ScheduleBroadcasts(queue, speed=speed)

        # Validate input argument.
        self.assertEqual(scheduler.speed, speed)
        with self.assertRaises(TypeError):
            ScheduleBroadcasts(queue, speed='speed')

    def test_start_stop(self):
        """Test ScheduleBroadcasts() start/stop."""

        # Create objects for scheduling data.
        read = ReadDirectory(LOG_PATH, message=True)
        data = BufferData(read)
        data.start()

        # Wait for buffer to fill.
        start_wait = time.time()
        while not data.is_ready() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Schedule data.
        scheduler = ScheduleBroadcasts(data.queue)

        # Start scheduling data.
        self.assertTrue(scheduler.start())
        self.assertTrue(scheduler.is_alive())
        self.assertFalse(scheduler.start())

        # Stop scheduling data.
        self.assertTrue(scheduler.stop())
        self.assertFalse(scheduler.is_alive())
        self.assertFalse(scheduler.stop())

        # Allow threads to fully shut down.
        time.sleep(0.1)

    def schedule(self, speed=None):
        """Schedule broadcasts."""

        # Listen for message broadcasts.
        listener_A = MessageListener(UnitTestMessageA)
        listener_B = MessageListener(UnitTestMessageB)
        buffer_A = list()
        buffer_B = list()

        # Subscribe callbacks.
        def callback_A(data): buffer_A.append(data)
        def callback_B(data): buffer_B.append(data)
        listener_A.subscribe(callback_A)
        listener_B.subscribe(callback_B)

        # Create objects for scheduling data.
        read = ReadDirectory(LOG_PATH, message=True)
        data = BufferData(read)
        data.start()

        # Wait for buffer to fill.
        start_wait = time.time()
        while not data.is_ready() and ((time.time() - start_wait) < TIMEOUT):
            time.sleep(0.1)

        # Schedule data.
        if not speed:
            speed = 1.0
            scheduler = ScheduleBroadcasts(data.queue)
        else:
            scheduler = ScheduleBroadcasts(data.queue, speed=speed)
        scheduler.start()

        # Wait for broadcasts to end.
        run_time = 1.0 / float(speed)
        start_wait = time.time()
        while True:
            duration = time.time() - start_wait
            if scheduler.is_alive() and duration < run_time:
                time.sleep(0.05)
            else:
                break

        # Ensure all messages were received.
        self.assertEqual(len(buffer_A) + len(buffer_B), 20)

        # Ensure timing is approximately correct.
        self.assertGreaterEqual(duration, 0.5 * run_time)
        self.assertLessEqual(duration, 2.0 * run_time)

    def test_schedule(self):
        """Test ScheduleBroadcasts() schedule data at normal speed."""

        # Replay data at normal speed.
        self.schedule()

    def test_schedule_speed(self):
        """Test ScheduleBroadcasts() schedule data at a faster speed."""

        # Replay data at double speed.
        self.schedule(speed=2)