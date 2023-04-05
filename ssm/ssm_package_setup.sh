main(){
#       load_runtime_dependencies
#    message "Load ci_fstcomp for developpement ..."
#    message '. ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/apps/ci_fstcomp/(check directory for latest version)/'
}

message(){
   echo $(tput -T xterm setaf 3)$@$(tput -T xterm sgr 0) >&2
   true
}
