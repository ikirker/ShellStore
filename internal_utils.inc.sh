#!/usr/bin/env bash

SSH_ARGS="-q -o PreferredAuthentications=publickey"
SCP_ARGS="$SSH_ARGS"

INIT() {
  mkdir -p "$RUNS_IN_DIR" || ERROR "while attempting to create store dir"
  mkdir -p "$TMP_RCV_DIR" || ERROR "while attempting to create rcv dir"
  if id $RUNS_AS_USER >/dev/null; then : ; else ERROR "intended user not found"; fi
  hostname >>$RUNS_IN_DIR/nodelist.txt
  CHECK_NODE_CONNECTIVITY
}

CHECK_NODE_CONNECTIVITY() {
  for node in `cat $RUNS_IN_DIR/nodelist.txt`; do
    node_test_output=`ssh $SSH_ARGS $node echo "test_output"`
    if [ -z "$node_test_output" ]; then
      ERROR "while checking node connectivity"
    else
      unset node_test_output
    fi
  done
}

PUT_NEW_OBJECT() {
  OBJECT_PREFIX=`echo "$RANDOM $RANDOM $RANDOM $RANDOM $RANDOM $RANDOM $RANDOM" | sha1sum | cut -f 1 -d" "`
  # Possible alternative:
  # OBJECT_PREFIX=`dd if=/dev/random bs=64 count=1 | sha1sum | cut -f 1 -d " "`

  # Adjust suffix-length for very large objects
  # 5: max size ~11TB
  gzip -c  - | split --bytes=1MB --suffix-length=5 - "$TMP_RCV_DIR/$OBJECT_PREFIX.gz."

  if [ $? -eq 0 ]; then
    DISTRIBUTE_FILES $TMP_RCV_DIR/$OBJECT_PREFIX.gz.*
    
    if [ $? -eq 0 ]; then
      rm $TMP_RCV_DIR/$OBJECT_PREFIX.gz.*
    echo "Success: $OBJECT_PREFIX" >&2
    else
      ERROR "while attempting to distribute files" 
    fi
  else
    ERROR "while creating shards: temporary files not deleted"
  fi
}

DISTRIBUTE_FILES () {
  while [ "$1" != "" ]; do 
    for i in `seq 1 $NUMBER_OF_REPLICAS`; do
      case "$DISTRIBUTION_ALGORITHM" in
        "random")
          node=`sort -R "$RUNS_IN_DIR/nodelist.txt" | head -n 1`
          ;;
        "mod_rotate")
          num_nodes=`wc -l "$RUNS_IN_DIR/nodelist.txt" | cut -f 1 -d ' '`
          basename=`basename "$1"`
          char="${basename:$((i-1)):$i}"
          node_line_no=$(( 1 + $(printf %d \'$char) % $num_nodes ))
          node=`sed -ne ${node_line_no}p $RUNS_IN_DIR/nodelist.txt`
          ;;
        *)
          ERROR "no valid distribution algorithm specified"
      esac
      SEND_FILE_TO_NODE "$node" "$1"
      if [ $? -ne 0 ]; then
        ERROR "while distributing files"
      fi
    done
    shift
  done
}

SEND_FILE_TO_NODE() {
  scp                                      \
   $SCP_ARGS                               \
   "$2" "$RUNS_AS_USER@$1:$RUNS_IN_DIR/"   \
   || ERROR "while attempting to send file to node"
}

GET_FILE_LIST_FROM_NODE() {
  ssh                                       \
   $SSH_ARGS                                \
   "$1" find "$RUNS_IN_DIR" -maxdepth 1 -name "*.gz.*" \
   || ERROR "while attempting to get file list from node"
}

GET_LIST_OF_ALL_OBJECTS() {
  all_lists=""
  for node in `cat $RUNS_IN_DIR/nodelist.txt`; do
    one_list=`GET_FILE_LIST_FROM_NODE "$node"`
    all_lists=`echo -e "${all_lists}${all_lists:+\n}${one_list}"`
  done
  all_lists=`sed -e 's/\.gz\..*//' -e "s_^.*/__" <<<"$all_lists" | sort -u`
  echo "$all_lists"
}


GET_OBJECT_CONTENTS() {
  object_prefix="$1"

  all_node_files=""

  # Note: this repeats get file list from node so that
  #  we don't have to transfer the entire file list over:
  #  the filtering is done on the node
  for node in `cat "$RUNS_IN_DIR/nodelist.txt"`; do
    node_files=`ssh \
                 $SSH_ARGS                                       \
                 "$node"                                         \
                 "find \"$RUNS_IN_DIR\" -maxdepth 1 -name \"*.gz.*\" \
                    | grep \"$object_prefix\"                        \
                    | sed -e \"s/^/$node /\""`

    all_node_files=`echo -ne "$node_files\n$all_node_files"`
  done

  node_object_sources=`echo "$all_node_files" \
                        | sort -R             \
                        | sort -k 2           \
                        | uniq -f 1           \
                        | tr ' ' ':'`
  # TODO: Add debug option for this sort of thing:
  echo "Getting object, sources: $node_object_sources" >&2
  (
  for entry in $node_object_sources; do
    target_node=${entry%%:*}
    target_file=${entry##*:}
    ssh \
     $SSH_ARGS                                 \
     "$RUNS_AS_USER@$target_node"              \
     cat "$target_file"
  done
  ) | gzip -d -c 
}

DELETE_OBJECT_ACROSS_NODES() {
  object_prefix="$1"

  for node in `cat "$RUNS_IN_DIR/nodelist.txt"`; do
    ssh \
     $SSH_ARGS                                 \
     "$RUNS_AS_USER@$node"                     \
     "rm $RUNS_IN_DIR/$object_prefix.gz.*" 
  done
}

ERROR() {
  echo "[`date +%Y-%m-%dT%H:%M:%S`] Error: $1" >&2
  exit 1
}


