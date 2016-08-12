var myApp = angular.module('myApp', ['ng-admin']);
myApp.config(['NgAdminConfigurationProvider', function (nga) {
    // create an admin application
    var admin = nga.application('DF Admin')
      .baseApiUrl('http://localhost:8080/api/df/'); // main API endpoint
    var user = nga.entity('ps');
    // set the fields of the user entity list view
    user.listView().fields([
        nga.field('id').isDetailLink(true),
        nga.field('taskId').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').label('Connector'),
        nga.field('connectorType').label('Connector Type'),
        nga.field('status').label('Job Status')
    ]);

        user.creationView().fields([
        nga.field('id'),
        nga.field('taskId').label('Task ID'),
        nga.field('name').label('Job Name'),
        nga.field('connector').label('Connector'),
        nga.field('connectorType').label('Connector Type'),
        nga.field('status').label('Job Status'),
        nga.field('jobConfig','json').label('Job Config'),
        nga.field('connectorConfig','json').label('Connector Config')
    ]);
    // use the same fields for the editionView as for the creationView
    user.editionView().fields(user.creationView().fields());

    // add the user entity to the admin application
    admin.addEntity(user);
    // attach the admin application to the DOM and execute it
    nga.configure(admin);
}]);
