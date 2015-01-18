<?php

/* Pass in by reference! */
function graph_namenode_report ( &$rrdtool_graph ) {

    global $conf,
           $context,
           $range,
           $rrd_dir,
           $size;

    if ($conf['strip_domainname']) {
       $hostname = strip_domainname($GLOBALS['hostname']);
    } else {
       $hostname = $GLOBALS['hostname'];
    }

    $title = 'Namenode Capacity';
    $rrdtool_graph['title'] = $title;
    $rrdtool_graph['lower-limit'] = '0';
    $rrdtool_graph['vertical-label'] = 'Bytes';
    $rrdtool_graph['extras'] = '--base 1024';
    $rrdtool_graph['height'] += ($size == 'medium') ? 28 : 0;

    if ( $conf['graphreport_stats'] ) {
        $rrdtool_graph['height'] += ($size == 'medium') ? 4 : 0;
        $rmspace = '\\g';
    } else {
        $rmspace = '';
    }
    $rrdtool_graph['extras'] .= ($conf['graphreport_stats'] == true) ? ' --font LEGEND:7' : '';

    if ($size == 'small') {
       $eol1 = '\\l';
       $space1 = ' ';
       $space2 = '         ';
    } else if ($size == 'medium' || $size = 'default') {
       $eol1 = '';
       $space1 = ' ';
       $space2 = '';
    } else if ($size == 'large') {
       $eol1 = '';
       $space1 = '                 ';
       $space2 = '                 ';
    }

    $series = "DEF:'nn_dfs_total_space'='${rrd_dir}/nn_dfs_total_space.rrd':'sum':AVERAGE "
        ."CDEF:'bnn_dfs_total_space'=nn_dfs_total_space,1,* "
        ."DEF:'nn_dfs_used_space'='${rrd_dir}/nn_dfs_used_space.rrd':'sum':AVERAGE "
        ."CDEF:'bnn_dfs_used_space'=nn_dfs_used_space,1024,* "
        ."DEF:'nn_dfs_remaining_space'='${rrd_dir}/nn_dfs_remaining_space.rrd':'sum':AVERAGE "
        ."CDEF:'bnn_dfs_remaining_space'=nn_dfs_remaining_space,1024,* ";

    $series .= "LINE2:'bnn_dfs_total_space'#${conf['cpu_num_color']}:'Capaciy Total${rmspace}' ";

    if ( $conf['graphreport_stats'] ) {
        $series .= "CDEF:total_pos=bnn_dfs_total_space,0,INF,LIMIT "
                . "VDEF:total_last=total_pos,LAST "
                . "VDEF:total_min=total_pos,MINIMUM " 
                . "VDEF:total_avg=total_pos,AVERAGE " 
                . "VDEF:total_max=total_pos,MAXIMUM " 
                . "GPRINT:'total_last':' ${space1}Now\:%6.1lf%s' "
                . "GPRINT:'total_min':'${space1}Min\:%6.1lf%s${eol1}' "
                . "GPRINT:'total_avg':'${space2}Avg\:%6.1lf%s' "
                . "GPRINT:'total_max':'${space1}Max\:%6.1lf%s\\l' ";
    }

    $series .= "AREA:'nn_dfs_used_space'#3333BB:'Capaciy Used${rmspace}' ";

    if ( $conf['graphreport_stats'] ) {
        $series .= "CDEF:used_pos=nn_dfs_used_space,0,INF,LIMIT "
                . "VDEF:used_last=used_pos,LAST "
                . "VDEF:used_min=used_pos,MINIMUM " 
                . "VDEF:used_avg=used_pos,AVERAGE " 
                . "VDEF:used_max=used_pos,MAXIMUM " 
                . "GPRINT:'used_last':' ${space1}Now\:%6.1lf%s' "
                . "GPRINT:'used_min':'${space1}Min\:%6.1lf%s${eol1}' "
                . "GPRINT:'used_avg':'${space2}Avg\:%6.1lf%s' "
                . "GPRINT:'used_max':'${space1}Max\:%6.1lf%s\\l' ";
    }
	
    $series .= "STACK:'nn_dfs_remaining_space'#33CC33:'Capaciy Remaining${rmspace}' ";

    if ( $conf['graphreport_stats'] ) {
        $series .= "CDEF:remaining_pos=nn_dfs_remaining_space,0,INF,LIMIT "
                . "VDEF:remaining_last=remaining_pos,LAST "
                . "VDEF:remaining_min=remaining_pos,MINIMUM " 
                . "VDEF:remaining_avg=remaining_pos,AVERAGE " 
                . "VDEF:remaining_max=remaining_pos,MAXIMUM " 
                . "GPRINT:'remaining_last':' ${space1}Now\:%6.1lf%s' "
                . "GPRINT:'remaining_min':'${space1}Min\:%6.1lf%s${eol1}' "
                . "GPRINT:'remaining_avg':'${space2}Avg\:%6.1lf%s' "
                . "GPRINT:'remaining_max':'${space1}Max\:%6.1lf%s\\l' ";
    }
	
    // If metrics like mem_used and mem_shared are not present we are likely not collecting them on this
    // host therefore we should not attempt to build anything and will likely end up with a broken
    // image. To avoid that we'll make an empty image
    if ( !file_exists("$rrd_dir/nn_dfs_total_space.rrd") ) 
      $rrdtool_graph[ 'series' ] = 'HRULE:1#FFCC33:"No matching metrics detected"';   
    else
      $rrdtool_graph[ 'series' ] = $series;

    return $rrdtool_graph;
}

?>
