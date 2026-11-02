# ironbird's OSAL configuration for the cpu1 target, layered over sample_defs/default_osconfig.cmake
# and sample_defs/native_osconfig.cmake (cFS includes ${cpu}_osconfig.cmake last).
#
# tools/cfs/build.sh copies this file into the checkout's sample_defs/.

#
# OSAL_CONFIG_QUEUE_MAX_DEPTH
# ---------------------------
#
# The deepest message queue OSAL accepts (OS_QUEUE_MAX_DEPTH), and so the deepest software-bus
# pipe. TO_LAB asks for a telemetry pipe this deep (cpu1_to_lab_internal_cfg_values.h in this
# directory keeps the two in step) so the 10k packets/s throughput test can ride out TO_LAB's task
# being off-CPU for tens of milliseconds. A non-root cFS still has OSAL truncate each queue to
# fs.mqueue.msg_max, so the pipe is only this deep where that sysctl is at least this value.
#
set(OSAL_CONFIG_QUEUE_MAX_DEPTH 1000)
