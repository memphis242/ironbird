# ib_test: ironbird's own cFS app. It exists to give the acceptance tests a variable-length
# command and telemetry packet that stock cFS does not have (a counted array and a tail string).
# tools/cfs/build.sh copies this directory into the cFS checkout's apps/ and lists it in the
# mission's applist; tools/cfs/run.sh adds it to the startup script when CFS_APPS names it.
project(IB_TEST C)

add_cfe_app(ib_test fsw/src/ib_test.c)
target_include_directories(ib_test PUBLIC fsw/src)
