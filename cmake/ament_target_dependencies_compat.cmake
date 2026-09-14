# Shim: ROS 2 Lyrical removed ament_target_dependencies(). ros1_bridge master
# still uses it at 9 call sites, including for the synthetic ros1_* packages
# that find_ros1_package() defines as plain variables (no imported targets).
# Reproduce the old semantics: prefer modern exported targets, fall back to
# the <pkg>_INCLUDE_DIRS / _LIBRARIES / _DEFINITIONS variables.
if(NOT COMMAND ament_target_dependencies)
  function(ament_target_dependencies target)
    cmake_parse_arguments(_atd "" "" "PUBLIC;PRIVATE;INTERFACE;SYSTEM" ${ARGN})
    set(_deps ${_atd_UNPARSED_ARGUMENTS} ${_atd_PUBLIC} ${_atd_PRIVATE} ${_atd_INTERFACE})
    foreach(_dep IN LISTS _deps)
      if(DEFINED ${_dep}_TARGETS)
        target_link_libraries(${target} ${${_dep}_TARGETS})
      elseif(TARGET ${_dep}::${_dep})
        target_link_libraries(${target} ${_dep}::${_dep})
      endif()
      if(DEFINED ${_dep}_INCLUDE_DIRS)
        target_include_directories(${target} SYSTEM PUBLIC ${${_dep}_INCLUDE_DIRS})
      endif()
      if(DEFINED ${_dep}_LIBRARIES)
        target_link_libraries(${target} ${${_dep}_LIBRARIES})
      endif()
      if(DEFINED ${_dep}_DEFINITIONS)
        target_compile_definitions(${target} PUBLIC ${${_dep}_DEFINITIONS})
      endif()
    endforeach()
  endfunction()
endif()
