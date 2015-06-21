This is a dumb sort-of-object-store. I blame @glennklockwood.

It uses split gzipped files spread and replicated across a list of nodes, with keyed passwordless scp/ssh for distribution and reading. It's written in bash (though I haven't checked for bashisms, it might be portable to dash/ksh/sh) and a fairly vanilla blend of command-line tools.


Still missing:
 
 * more careful replication, where we actually have multiple replicas on single hosts (or else make sure we don't try to make more replicas than we have hosts: at the moment they just get overwritten on the same host)
 * nodelist syncing
 * ability to run in different dirs on different nodes
 * any sort of checking that what you get out is what you put in
 * probably a bunch of other things, I don't know

