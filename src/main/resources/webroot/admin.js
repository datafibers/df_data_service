var myApp = angular.module('myApp', ['ng-admin']);
myApp.config(['NgAdminConfigurationProvider', function (nga) {
customHeaderTemplate =
'<div class="navbar-header">' +
    '<a class="navbar-brand" href="#" ng-click="appController.displayHome()">' +
        'DataFibers Web Console' +
    '</a>' +
'</div>' +
'<p class="navbar-text navbar-right">' +
    '<a href="https://github.com/datafibers/df_data_processor/">' +
        '<img src="https://raw.githubusercontent.com/datafibers/datafibers.github.io/master/img/logos/logo_blue.png" width="24" height="28">' +
    '</a>' +
'</p>';
    // create an admin application
    var admin = nga.application().title('DataFibers Admin Console').baseApiUrl('http://localhost:8080/api/df/');
    admin.header(customHeaderTemplate);
	var processor = nga.entity('processor').label('ALL');
    var producer = nga.entity('ps').label('CONNECTS');
	var transformer = nga.entity('tr').label('TRANSFORMS');	
    var installed_connects = nga.entity('installed_connects').identifier(nga.field('class')).label('INSTALLED').readOnly();

// set the fields of the producer entity list view
    producer.listView().fields([
        nga.field('id').label('Job ID').isDetailLink(true),
        nga.field('taskId').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').label('Processor'),
        nga.field('connectorType').label('Type'),
        nga.field('status').label('Job Status')
    ]);
    
    // set the fields of the transformer entity list view
    transformer.listView().fields([
        nga.field('id').label('Job ID').isDetailLink(true),
        nga.field('taskId').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').label('Processor'),
        nga.field('connectorType').label('Type'),
        nga.field('status').label('Job Status')
    ]);

    producer.listView().title('Connects Dashboard');
	
    transformer.listView().title('Transforms Dashboard');

    producer.creationView().fields([
        nga.field('taskId').label('Task ID','number'),
        nga.field('name').label('Job Name'),
        nga.field('connector').attributes({placeholder:'No space allowed and 5 chars min.'}).validation({ required: true, pattern: '[A-Za-z0-9\-]{5,20}' }).label('Connects'),
        nga.field('connectorType', 'choice')
                .choices([
                                {value:'KAFKA_SOURCE', label:'KAFKA_SOURCE'},
                                {value:'KAFKA_SINK', label:'KAFKA_SINK'},
                                {value:'HDFS_SOURCE', label:'HDFS_SOURCE'},
                                {value:'HDFS_SINK', label:'HDFS_SINK'},
                                {value:'HIVE_SOURCE', label:'HIVE_SOURCE'},
                                {value:'HIVE_SINK', label:'HIVE_SINK'},
                                {value:'NONE', label:'NONE'}]).label('Connector Type'),
        nga.field('status').editable(false).label('Job Status'),
        nga.field('description', 'text'),
        nga.field('jobConfig','json').attributes({placeholder:'Json format of job configuration is request.'}).label('Job Config'),
        nga.field('connectorConfig','json').attributes({placeholder:'Json format of connects configuration is request.'}).label('Connects Config')
    ]);
	
	transformer.creationView().fields([
        nga.field('taskId').label('Task ID','number'),
        nga.field('name').label('Job Name'),
        nga.field('connector').attributes({placeholder:'No space allowed and 5 chars min.'}).validation({ required: true, pattern: '[A-Za-z0-9\-]{5,20}' }).label('Transforms'),
        nga.field('connectorType', 'choice')
                .choices([
                                {value:'FLINK_TRANS', label:'FLINK_TRANS'},
                                {value:'FLINK_JOINS', label:'FLINK_JOINS'},
                                {value:'SPARK_TRANS', label:'SPARK_TRANS'},
                                {value:'SPARK_JOINS', label:'SPARK_JOINS'},
                                {value:'HIVE_TRANS', label:'HIVE_TRANS'},
                                {value:'HIVE_JOINS', label:'HIVE_JOINS'},
                                {value:'NONE', label:'NONE'}]).label('Transforms Type'),
        nga.field('status').editable(false).label('Job Status'),
        nga.field('description', 'text'),
        nga.field('jobConfig','json').attributes({placeholder:'Json format of job configuration is request.'}).label('Job Config'),
        nga.field('connectorConfig','json').attributes({placeholder:'Json format of transforms configuration is request.'}).label('Transforms Config')
    ]);
	
	
    // use the same fields for the editionView as for the creationView
    producer.editionView().fields(producer.creationView().fields());
	transformer.editionView().fields(transformer.creationView().fields());
	
	// set the fields of the proceesor entity list view
    processor.listView().fields([
        nga.field('id').label('Job ID').isDetailLink(true),
        nga.field('taskId').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').label('Processor'),
        nga.field('connectorType').label('Type'),
		nga.field('connectorCategory').label('Category'),
        nga.field('status').label('Job Status')
    ]);
    processor.listView().title('All Connects and Transforms');
    processor.listView().batchActions([])

    // set the fields of the producer entity list view
    installed_connects.listView().fields([
        nga.field('class').label('Connects')
    ]);
    installed_connects.listView().title('Connects Installed');
    installed_connects.listView().batchActions([])
	
    // add the producer entity to the admin application
    admin.addEntity(processor).addEntity(producer).addEntity(transformer).addEntity(installed_connects);
	
	// customize menubar
	admin.menu(nga.menu()
  .addChild(nga.menu(processor).icon('<span class="fa fa-globe fa-fw"></span>'))
  .addChild(nga.menu(producer).icon('<span class="fa fa-plug fa-fw"></span>'))
  .addChild(nga.menu(transformer).icon('<span class="fa fa-flask fa-fw"></span>'))
  .addChild(nga.menu(installed_connects).icon('<span class="fa fa-cog fa-fw"></span>'))
);
    // attach the admin application to the DOM and execute it
    nga.configure(admin);
}]);

