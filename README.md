The project is a hotel platform for users to search the ideal hotel.
It has two microservice. hotel-admin is for managing the records in MySQL database. And hotel-demo is for displaying hotel list from Elasticsearch.
The RabbitMQ is used to send updated message of MySQL from admin to demo.