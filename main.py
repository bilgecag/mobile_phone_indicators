import migration_detector as md
from .core import find_segment, fill_missing_day, filter_seg_appear_prop
from .traj_utils import *
traj = md.read_csv("/Users/bilgecagaydogdu/Desktop/EARTHQUAKE/mobile_phone_indicators/mig_detector.csv")
#migrants = traj.find_migrants()
traj.user_traj['filled_record'] = traj.user_traj['all_record'].apply(lambda x: fill_missing_day(x, 7))


