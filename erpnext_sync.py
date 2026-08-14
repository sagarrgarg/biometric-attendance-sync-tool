
import local_config as config
import requests
import datetime
import json
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from pickledb import PickleDB
from zk import ZK, const

EMPLOYEE_NOT_FOUND_ERROR_MESSAGE = "No Employee found for the given employee field value"
EMPLOYEE_INACTIVE_ERROR_MESSAGE = "Transactions cannot be created for an Inactive Employee"
DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE = "This employee already has a log with the same timestamp"
CHECKIN_BEFORE_JOINING_ERROR_MESSAGE = "cannot be before employee"
allowlisted_errors = [EMPLOYEE_NOT_FOUND_ERROR_MESSAGE, EMPLOYEE_INACTIVE_ERROR_MESSAGE, DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE, CHECKIN_BEFORE_JOINING_ERROR_MESSAGE]

if hasattr(config, 'allowed_exceptions'):
    allowlisted_errors_temp = []
    for error_number in config.allowed_exceptions:
        if 1 <= error_number <= len(allowlisted_errors):
            allowlisted_errors_temp.append(allowlisted_errors[error_number - 1])
    allowlisted_errors = allowlisted_errors_temp

device_punch_values_IN = getattr(config, 'device_punch_values_IN', [0,4])
device_punch_values_OUT = getattr(config, 'device_punch_values_OUT', [1,5])
ERPNEXT_VERSION = getattr(config, 'ERPNEXT_VERSION', 14)

def _default_checkin_url():
    endpoint_app = "hrms" if ERPNEXT_VERSION > 13 else "erpnext"
    return f"{config.ERPNEXT_URL}/api/method/{endpoint_app}.hr.doctype.employee_checkin.employee_checkin.add_log_based_on_employee_field"

def _default_shift_type_url():
    # {shift_type} is URL-quoted at call time
    return f"{config.ERPNEXT_URL}/api/resource/Shift Type/{{shift_type}}"

# Endpoint overrides (optional). If unset, fall back to the standard ERPNext paths above.
#   ERPNEXT_CHECKIN_URL: full URL to POST punch logs to.
#   ERPNEXT_SHIFT_TYPE_URL: full URL to PUT shift sync timestamps to. Use the literal
#     '{shift_type}' placeholder where the shift name should be substituted.
ERPNEXT_CHECKIN_URL = getattr(config, 'ERPNEXT_CHECKIN_URL', None) or _default_checkin_url()
ERPNEXT_SHIFT_TYPE_URL = getattr(config, 'ERPNEXT_SHIFT_TYPE_URL', None) or _default_shift_type_url()
# (connect, read) timeout in seconds for every ERPNext HTTP call. Without this,
# requests waits on the OS default (~20s+) on an unreachable server, and does so
# for every single punch — turning one outage into a very long hang. Override in
# local_config with REQUEST_TIMEOUT = (connect, read) or a single number.
REQUEST_TIMEOUT = getattr(config, 'REQUEST_TIMEOUT', (10, 30))

# possible area of further developemt
    # Real-time events - setup getting events pushed from the machine rather then polling.
        #- this is documented as 'Real-time events' in the ZKProtocol manual.

# Notes:
# Status Keys in status.json
#  - lift_off_timestamp
#  - mission_accomplished_timestamp
#  - <device_id>_pull_timestamp
#  - <device_id>_push_timestamp
#  - <shift_type>_sync_timestamp

def main():
    """Takes care of checking if it is time to pull data based on config,
    then calling the relevent functions to pull data and push to EPRNext.

    """
    try:
        now = datetime.datetime.now()
        last_lift_off_timestamp = _safe_convert_date(status.get('lift_off_timestamp'), "%Y-%m-%d %H:%M:%S.%f")
        # Treat a timestamp in the future (clock skew / corrupted value) as if it were missing,
        # otherwise the gating condition stays False forever and the script silently never runs.
        if last_lift_off_timestamp and last_lift_off_timestamp > now:
            info_logger.warning(f'lift_off_timestamp {last_lift_off_timestamp} is in the future; ignoring it')
            last_lift_off_timestamp = None
        if (last_lift_off_timestamp and last_lift_off_timestamp < now - datetime.timedelta(minutes=config.PULL_FREQUENCY)) or not last_lift_off_timestamp:
            status.set('lift_off_timestamp', str(now))
            status.save()
            info_logger.info("Cleared for lift off!")
            try:
                for device in config.devices:
                    device_attendance_logs = None
                    info_logger.info("Processing Device: "+ device['device_id'])
                    dump_file = get_dump_file_name_and_directory(device['device_id'], device['ip'])
                    if os.path.exists(dump_file):
                        info_logger.warning('Device Attendance Dump Found in Log Directory. This can mean the program crashed unexpectedly. Retrying with dumped data.')
                        with open(dump_file, 'r') as f:
                            file_contents = f.read()
                            if file_contents:
                                device_attendance_logs = list(map(lambda x: _apply_function_to_key(x, 'timestamp', datetime.datetime.fromtimestamp), json.loads(file_contents)))
                    try:
                        pull_process_and_push_data(device, device_attendance_logs)
                        status.set(f'{device["device_id"]}_push_timestamp', str(datetime.datetime.now()))
                        status.save()
                        if os.path.exists(dump_file):
                            os.remove(dump_file)
                        info_logger.info("Successfully processed Device: "+ device['device_id'])
                    except:
                        error_logger.exception('exception when calling pull_process_and_push_data function for device'+json.dumps(device, default=str))
                if hasattr(config,'shift_type_device_mapping'):
                    update_shift_last_sync_timestamp(config.shift_type_device_mapping)
                status.set('mission_accomplished_timestamp', str(datetime.datetime.now()))
                status.save()
                info_logger.info("Mission Accomplished!")
            except:
                # Clear lift_off so the next tick retries instead of waiting a full PULL_FREQUENCY
                # window — the prior behavior silently locked the script out for up to 60 minutes
                # whenever any uncaught error escaped the device loop.
                status.set('lift_off_timestamp', None)
                status.save()
                raise
    except:
        error_logger.exception('exception has occurred in the main function...')


def pull_process_and_push_data(device, device_attendance_logs=None):
    """ Takes a single device config as param and pulls data from that device.

    params:
    device: a single device config object from the local_config file
    device_attendance_logs: fetching from device is skipped if this param is passed. used to restart failed fetches from previous runs.
    """
    attendance_success_log_file = '_'.join(["attendance_success_log", device['device_id']])
    attendance_failed_log_file = '_'.join(["attendance_failed_log", device['device_id']])
    attendance_success_logger = setup_logger(attendance_success_log_file, '/'.join([config.LOGS_DIRECTORY, attendance_success_log_file])+'.log')
    attendance_failed_logger = setup_logger(attendance_failed_log_file, '/'.join([config.LOGS_DIRECTORY, attendance_failed_log_file])+'.log')
    if not device_attendance_logs:
        device_attendance_logs = get_all_attendance_from_device(device['ip'], device_id=device['device_id'], clear_from_device_on_fetch=device['clear_from_device_on_fetch'])
        if not device_attendance_logs:
            return
    # for finding the last successfull push and restart from that point (or) from a set 'config.IMPORT_START_DATE' (whichever is later)
    index_of_last = -1
    last_line = get_last_line_from_file('/'.join([config.LOGS_DIRECTORY, attendance_success_log_file])+'.log')
    import_start_date = _safe_convert_date(config.IMPORT_START_DATE, "%Y%m%d")
    if last_line or import_start_date:
        last_user_id = None
        last_timestamp = None
        if last_line:
            last_user_id, last_timestamp = last_line.split("\t")[4:6]
            last_timestamp = datetime.datetime.fromtimestamp(float(last_timestamp))
        if import_start_date:
            if last_timestamp:
                if last_timestamp < import_start_date:
                    last_timestamp = import_start_date
                    last_user_id = None
            else:
                last_timestamp = import_start_date
        for i, x in enumerate(device_attendance_logs):
            if last_user_id and last_timestamp:
                if last_user_id == str(x['user_id']) and last_timestamp == x['timestamp']:
                    index_of_last = i
                    break
            elif last_timestamp:
                if x['timestamp'] >= last_timestamp:
                    index_of_last = i
                    break

    for device_attendance_log in device_attendance_logs[index_of_last+1:]:
        punch_direction = device['punch_direction']
        if punch_direction == 'AUTO':
            if device_attendance_log['punch'] in device_punch_values_OUT:
                punch_direction = 'OUT'
            elif device_attendance_log['punch'] in device_punch_values_IN:
                punch_direction = 'IN'
            else:
                punch_direction = None
        erpnext_status_code, erpnext_message = send_to_erpnext(device_attendance_log['user_id'], device_attendance_log['timestamp'], device['device_id'], punch_direction, latitude=device.get('latitude'), longitude=device.get('longitude'))
        if erpnext_status_code == 200:
            attendance_success_logger.info("\t".join([erpnext_message, str(device_attendance_log['uid']),
                str(device_attendance_log['user_id']), str(device_attendance_log['timestamp'].timestamp()),
                str(device_attendance_log['punch']), str(device_attendance_log['status']),
                json.dumps(device_attendance_log, default=str)]))
        else:
            attendance_failed_logger.error("\t".join([str(erpnext_status_code), str(device_attendance_log['uid']),
                str(device_attendance_log['user_id']), str(device_attendance_log['timestamp'].timestamp()),
                str(device_attendance_log['punch']), str(device_attendance_log['status']),
                json.dumps(device_attendance_log, default=str)]))
            if not(any(error in erpnext_message for error in allowlisted_errors)):
                raise Exception('API Call to ERPNext Failed.')


def get_all_attendance_from_device(ip, port=4370, timeout=30, device_id=None, clear_from_device_on_fetch=False):
    #  Sample Attendance Logs [{'punch': 255, 'user_id': '22', 'uid': 12349, 'status': 1, 'timestamp': datetime.datetime(2019, 2, 26, 20, 31, 29)},{'punch': 255, 'user_id': '7', 'uid': 7, 'status': 1, 'timestamp': datetime.datetime(2019, 2, 26, 20, 31, 36)}]
    zk = ZK(ip, port=port, timeout=timeout)
    conn = None
    attendances = []
    try:
        conn = zk.connect()
        x = conn.disable_device()
        # device is disabled when fetching data
        info_logger.info("\t".join((ip, "Device Disable Attempted. Result:", str(x))))
        attendances = conn.get_attendance()
        info_logger.info("\t".join((ip, "Attendances Fetched:", str(len(attendances)))))
        status.set(f'{device_id}_push_timestamp', None)
        status.set(f'{device_id}_pull_timestamp', str(datetime.datetime.now()))
        status.save()
        if len(attendances):
            # keeping a backup before clearing data incase the programs fails.
            # if everything goes well then this file is removed automatically at the end.
            dump_file_name = get_dump_file_name_and_directory(device_id, ip)
            with open(dump_file_name, 'w+') as f:
                f.write(json.dumps(list(map(lambda x: x.__dict__, attendances)), default=datetime.datetime.timestamp))
            if clear_from_device_on_fetch:
                x = conn.clear_attendance()
                info_logger.info("\t".join((ip, "Attendance Clear Attempted. Result:", str(x))))
        x = conn.enable_device()
        info_logger.info("\t".join((ip, "Device Enable Attempted. Result:", str(x))))
    except:
        error_logger.exception(str(ip)+' exception when fetching from device...')
        raise Exception('Device fetch failed.')
    finally:
        if conn:
            conn.disconnect()
    return list(map(lambda x: x.__dict__, attendances))


def wipe_device_attendance(device_id):
    """Erase all attendance logs from the biometric device identified by
    `device_id` in local_config.devices. Users and fingerprints are left
    intact. Intended to be invoked from the CLI, independently of the
    sync loop."""
    device = next((d for d in config.devices if d['device_id'] == device_id), None)
    if not device:
        raise SystemExit(f"device_id {device_id!r} not found in local_config.devices")
    ip = device['ip']
    zk = ZK(ip, port=4370, timeout=30)
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()
        info_logger.info("\t".join((ip, "Device Disable Attempted (wipe).")))
        result = conn.clear_attendance()
        info_logger.info("\t".join((ip, "Attendance Clear Attempted. Result:", str(result))))
        conn.enable_device()
        info_logger.info("\t".join((ip, "Device Enable Attempted (wipe).")))
        print(f"Cleared attendance on device {device_id} ({ip}). Result: {result}")
    except Exception:
        error_logger.exception(f"{ip} exception when wiping device {device_id}...")
        raise
    finally:
        if conn:
            conn.disconnect()


def push_dump_to_erpnext(device_id):
    """Replay the on-disk attendance dump for `device_id` to ERPNext without
    touching the biometric device. Useful when a dump was left behind (e.g. a
    crash) or when you want to re-push fetched logs manually.

    The dump path is derived from the device's `ip` in local_config.devices,
    matching get_dump_file_name_and_directory(). Records are pushed through
    pull_process_and_push_data(), so the per-device success log and
    IMPORT_START_DATE still apply — already-synced punches are skipped, not
    duplicated. On success the dump file is removed, mirroring the sync loop."""
    device = next((d for d in config.devices if d['device_id'] == device_id), None)
    if not device:
        raise SystemExit(f"device_id {device_id!r} not found in local_config.devices")
    dump_file = get_dump_file_name_and_directory(device['device_id'], device['ip'])
    if not os.path.exists(dump_file):
        raise SystemExit(f"no dump file found at {dump_file}")
    with open(dump_file, 'r') as f:
        file_contents = f.read()
    if not file_contents.strip():
        raise SystemExit(f"dump file {dump_file} is empty; nothing to push")
    device_attendance_logs = list(map(
        lambda x: _apply_function_to_key(x, 'timestamp', datetime.datetime.fromtimestamp),
        json.loads(file_contents)))
    print(f"Pushing {len(device_attendance_logs)} logs from {dump_file} for device {device_id}...")
    pull_process_and_push_data(device, device_attendance_logs)
    status.set(f'{device_id}_push_timestamp', str(datetime.datetime.now()))
    status.save()
    os.remove(dump_file)
    print(f"Done. Pushed dump for {device_id} and removed {dump_file}.")


def send_to_erpnext(employee_field_value, timestamp, device_id=None, log_type=None, latitude=None, longitude=None):
    """
    Examples: 
    
    For ERPNext, Frappe HR <= v14
    send_to_erpnext('12349',datetime.datetime.now(),'HO1','IN')

    For ERPNext, Frappe HR v15 onwards
    If 'Allow Geolocation Tracking' is on
    send_to_erpnext('12349',datetime.datetime.now(),'HO1','IN',latitude=12.34, longitude=56.78)
    """
    headers = {
        'Authorization': "token "+ config.ERPNEXT_API_KEY + ":" + config.ERPNEXT_API_SECRET,
        'Accept': 'application/json'
    }
    data = {
        'employee_field_value' : employee_field_value,
        'timestamp' : timestamp.__str__(),
        'device_id' : device_id,
        'log_type' : log_type,
        'latitude' : latitude,
        'longitude' : longitude
    }
    response = requests.request("POST", ERPNEXT_CHECKIN_URL, headers=headers, json=data, timeout=REQUEST_TIMEOUT)
    if response.status_code == 200:
        return 200, json.loads(response._content)['message']['name']
    else:
        error_str = _safe_get_error_str(response)
        error_logger.error('\t'.join(['Error during ERPNext API Call.', str(employee_field_value), str(timestamp.timestamp()), str(device_id), str(log_type), error_str]))
        return response.status_code, error_str

def update_shift_last_sync_timestamp(shift_type_device_mapping):
    """
    ### algo for updating the sync_current_timestamp
    - get a list of devices to check
    - check if all the devices have a non 'None' push_timestamp
        - check if the earliest of the pull timestamp is greater than sync_current_timestamp for each shift name
            - then update this min of pull timestamp to the shift

    """
    for shift_type_device_map in shift_type_device_mapping:
        all_devices_pushed = True
        pull_timestamp_array = []
        for device_id in shift_type_device_map['related_device_id']:
            if not status.get(f'{device_id}_push_timestamp'):
                all_devices_pushed = False
                break
            pull_timestamp_array.append(_safe_convert_date(status.get(f'{device_id}_pull_timestamp'), "%Y-%m-%d %H:%M:%S.%f"))
        if all_devices_pushed:
            min_pull_timestamp = min(pull_timestamp_array)
            if isinstance(shift_type_device_map['shift_type_name'], str): # for backward compatibility of config file
                shift_type_device_map['shift_type_name'] = [shift_type_device_map['shift_type_name']]
            for shift in shift_type_device_map['shift_type_name']:
                try:
                    sync_current_timestamp = _safe_convert_date(status.get(f'{shift}_sync_timestamp'), "%Y-%m-%d %H:%M:%S.%f")
                    if (sync_current_timestamp and min_pull_timestamp > sync_current_timestamp) or (min_pull_timestamp and not sync_current_timestamp):
                        response_code = send_shift_sync_to_erpnext(shift, min_pull_timestamp)
                        if response_code == 200:
                            status.set(f'{shift}_sync_timestamp', str(min_pull_timestamp))
                            status.save()
                except:
                    error_logger.exception('Exception in update_shift_last_sync_timestamp, for shift:'+shift)

def send_shift_sync_to_erpnext(shift_type_name, sync_timestamp):
    from urllib.parse import quote
    if '{shift_type}' in ERPNEXT_SHIFT_TYPE_URL:
        url = ERPNEXT_SHIFT_TYPE_URL.replace('{shift_type}', quote(shift_type_name, safe=''))
    else:
        url = ERPNEXT_SHIFT_TYPE_URL.rstrip('/') + '/' + quote(shift_type_name, safe='')
    headers = {
        'Authorization': "token "+ config.ERPNEXT_API_KEY + ":" + config.ERPNEXT_API_SECRET,
        'Accept': 'application/json'
    }
    data = {
        "last_sync_of_checkin" : str(sync_timestamp)
    }
    try:
        response = requests.request("PUT", url, headers=headers, data=json.dumps(data), timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            info_logger.info("\t".join(['Shift Type last_sync_of_checkin Updated', str(shift_type_name), str(sync_timestamp.timestamp())]))
        else:
            error_str = _safe_get_error_str(response)
            error_logger.error('\t'.join(['Error during ERPNext Shift Type API Call.', str(shift_type_name), str(sync_timestamp.timestamp()), error_str]))
        return response.status_code
    except:
        error_logger.exception("\t".join(['exception when updating last_sync_of_checkin in Shift Type', str(shift_type_name), str(sync_timestamp.timestamp())]))

def get_last_line_from_file(file):
    # concerns to address(may be much later):
        # how will last line lookup work with log rotation when a new file is created?
            #- will that new file be empty at any time? or will it have a partial line from the previous file?
    line = None
    if os.stat(file).st_size < 5000:
        # quick hack to handle files with one line
        with open(file, 'r') as f:
            for line in f:
                pass
    else:
        # optimized for large log files
        with open(file, 'rb') as f:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
            line = f.readline().decode()
    return line


def setup_logger(name, log_file, level=logging.INFO, formatter=None):

    if not formatter:
        formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

    handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=50)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger

def get_dump_file_name_and_directory(device_id, device_ip):
    return config.LOGS_DIRECTORY + '/' + device_id + "_" + device_ip.replace('.', '_') + '_last_fetch_dump.json'

def _apply_function_to_key(obj, key, fn):
    obj[key] = fn(obj[key])
    return obj

def _safe_convert_date(datestring, pattern):
    try:
        return datetime.datetime.strptime(datestring, pattern)
    except:
        return None

def _safe_get_error_str(res):
    try:
        error_json = json.loads(res._content)
        if 'exc' in error_json: # this means traceback is available
            error_str = json.loads(error_json['exc'])[0]
        else:
            error_str = json.dumps(error_json)
    except:
        error_str = str(res.__dict__)
    return error_str

# setup logger and status
if not os.path.exists(config.LOGS_DIRECTORY):
    os.makedirs(config.LOGS_DIRECTORY)
error_logger = setup_logger('error_logger', '/'.join([config.LOGS_DIRECTORY, 'error.log']), logging.ERROR)
info_logger = setup_logger('info_logger', '/'.join([config.LOGS_DIRECTORY, 'logs.log']))

def _open_status(path):
    # pickledb >= 1.0 auto-loads the file in __init__ and persists via save();
    # the old load()/db API was removed there. Validate the existing file
    # ourselves (plain JSON read, no version-specific internals) and, if it is
    # empty or holds malformed/non-dict JSON, quarantine it and start fresh —
    # otherwise stale or invalid state can lock the script up or make PickleDB
    # raise on construction.
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            with open(path, 'rb') as f:
                # decode as utf-8-sig so a BOM (common when a file is touched on
                # Windows) is stripped rather than mistaken for corruption.
                data = json.loads(f.read().decode('utf-8-sig'))
            if not isinstance(data, dict):
                raise ValueError(f'status file root is {type(data).__name__}, expected dict')
        except Exception:
            backup = path + '.corrupt-' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            try:
                os.rename(path, backup)
                error_logger.exception(f'status file at {path} was unreadable; quarantined to {backup} and starting fresh')
            except OSError:
                error_logger.exception(f'status file at {path} was unreadable and could not be quarantined; starting fresh')
    return PickleDB(path)

status = _open_status('/'.join([config.LOGS_DIRECTORY, 'status.json']))

def infinite_loop(sleep_time=15):
    print("Service Running...")
    while True:
        try:
            main()
            time.sleep(sleep_time)
        except BaseException as e:
            print(e)

if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "wipe":
        if len(sys.argv) < 3:
            raise SystemExit("usage: python erpnext_sync.py wipe <device_id>")
        wipe_device_attendance(sys.argv[2])
    elif len(sys.argv) >= 2 and sys.argv[1] == "push_dump":
        if len(sys.argv) < 3:
            raise SystemExit("usage: python erpnext_sync.py push_dump <device_id>")
        push_dump_to_erpnext(sys.argv[2])
    else:
        infinite_loop()
