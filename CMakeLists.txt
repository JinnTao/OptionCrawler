set( PYTHON_MODULE_ROOT $ENV{LONGBEACH_ROOT}/lib/python/longbeach )
set( PYTHON_SCRIPTS_DIR /with/longbeach/scripts/lib/python/longbeach )
set( PYTHON_MODULE_SCRIPTS_ROOT ${PYTHON_MODULE_ROOT}/scripts )

file( GLOB PYTHONLIBFILES *.py )

copy_rule( ${PYTHON_MODULE_ROOT} "${PYTHONLIBFILES}" TARGET_FILES )



add_custom_target( longbeach_python_base_libs ALL
#   COMMAND
#   ${CMAKE_COMMAND} -E create_symlink ${PYTHON_SCRIPTS_DIR} ${PYTHON_MODULE_SCRIPTS_ROOT}
  DEPENDS
  ${TARGET_FILES}
  )

add_dependencies( longbeach_python_base_libs
  Pytrion)
