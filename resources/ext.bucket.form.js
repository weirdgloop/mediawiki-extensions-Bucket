$(() => {
	// Infuse the widget for autocompletion
	mw.loader.using( [ 'oojs-ui-core', 'mediawiki.widgets' ], ( require ) => {
		$( '.mw-widget-titleInputWidget' ).each( ( index, element ) => {
			OO.ui.infuse( $( element ) );
		} );
	} );
} );
