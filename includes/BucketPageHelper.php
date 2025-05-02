<?php

namespace MediaWiki\Extension\Bucket;
use OOUI;

class BucketPageHelper {
    public static function getPageLinks($title, $limit, $offset, $query) {
        $links = [];

        $previousOffset = max(0, $offset-$limit);
        $links[] = new OOUI\ButtonWidget( [
            'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => max(0, $previousOffset) ] + $query ),
            'title' => wfMessage("bucket-previous") . " $limit results.",
            'label' => wfMessage("bucket-previous") . " $limit"
        ]);
        if ( $offset-$limit < 0 ) {
            end($links)->setDisabled(true);
        }

        foreach ( [20, 50, 100, 250, 500 ] as $num ) {
            $query = [ 'limit' => $num, 'offset' => $offset ] + $query;
            $tooltip = "Show $num results per page.";
            $links[] = new OOUI\ButtonWidget( [
                'href' => $title->getLocalURL($query),
                'title' => $tooltip,
                'label' => $num,
                'active' => ($num == $limit)
            ]);
        }

        $links[] = new OOUI\ButtonWidget( [
            'href' => $title->getLocalURL( [ 'limit' => $limit, 'offset' => $offset+$limit ] + $query ),
            'title' => wfMessage("bucket-next") . " $limit results.",
            'label' => wfMessage("bucket-next") . " $limit"
        ]);

        return new OOUI\ButtonGroupWidget( [ 'items' => $links ] );
    } 
}