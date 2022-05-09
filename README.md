# chatbot

An AWS based Dining Concierge Chatbot
S3 BUCKET LINK FOR THE CHATBOT : http://awsdiningchatbot.s3-website-us-east-1.amazonaws.com/

# INSTRUCTIONS :
Visit the link and chat with the bot. The initial response sent by the bot will be in your inbox or spam mail. This mail will be a verification mail, move the mail to inbox if in spam and verify your e-mail address by clicking on the link sent in the mail. After this step, refresh the page and chat with the bot one more time. You will now get a suggestion of resturants on your mail. NOTE: This process must be done for new E-mail ID. If the e-mail has been used before, verification is not needed, suggestion will be mailed directly. The bot only accepts future dates like tomorrow. (today, yesterday or any past date will not give results.)

# DESCRIPTION:
Customer Service is a core service for a lot of businesses around the world and it is getting disrupted at the moment by Natural Language Processing-powered applications. In this assignmet we have implemented a serverless, microservice-driven web-based Dining Concierge chatbot that sends you restaurant suggestions given a set of preferences that you provide the chatbot with through conversation.

Chatbot validates the responses from user and based on this conversation, the LEX chatbot will identify the customer’s preferred ‘cuisine’. The chatbot will search through ElasticSearch to get random suggestions of restaurant IDs with this cuisine. The chatbot will also query the DynamoDB table with these restaurant IDs to find more information about the restaurants you want to suggest to your customers like name and address of the restaurant. The chatbot will filter the restaurants based on the Cuisine. After that the chatbot will send the suggestions to user via Email.

# SERVICES USED
# AWS SERVICES

▫️ AWS S3 BUCKET:
Amazon Simple Storage Service (Amazon S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance. Object storage service that offers industry-leading scalability, data availability, security, and performance.

▫️ AWS LEX:
Amazon Lex is a fully managed artificial intelligence (AI) service with advanced natural language models for building conversational interfaces into applications such as Build virtual agents and voice assistants ,Automate informational responses ,and Improve productivity with application bots.

▫️ AWS LAMBDA :
AWS Lambda is a serverless compute service that lets you run code without provisioning or managing servers, creating workload-aware cluster scaling logic, maintaining event integrations, or managing runtimes. With Lambda, we ran code for the application with zero administration.

▫️ AWS API GATEWAY:
Amazon API Gateway is a fully managed service that makes it easy for developers to create, publish, maintain, monitor, and secure APIs at any scale. APIs act as the "front door" for applications to access data, business logic, or functionality from your backend services. Using API Gateway, we were able to create RESTful APIs for the chatbot which communicate with the lambda functions.

▫️ AWS SIMPLE QUEUE SERVICE (SQS):
Amazon Simple Queue Service (SQS) is a fully managed message queuing service that enables you to decouple and scale microservices, distributed systems, and serverless applications. Using SQS, we sent, stored, and received messages between lambda functions without losing messages or requiring other services to be available. We used Standard message queue for best throughput, best-effort ordering, and at-least-once delivery

▫️ AWS SIMPLE NOTIFICATION SERVICE (SES):
Amazon Simple Email Service (SES) is a cost-effective, flexible, and scalable email service that enables developers to send mail from within any application.

▫️ AWS Elasticsearch :
AWS Elasticsearch is a distributed search and analytics engine built on Apache Lucene. Since its release in 2010, Elasticsearch has quickly become the most popular search engine and is commonly used for log analytics, full-text search, security intelligence, business analytics, and operational intelligence use cases.

▫️ AWS DynamoDB:
Amazon DynamoDB is a fully managed, serverless, key-value NoSQL database designed to run high-performance applications at any scale. DynamoDB offers built-in security, continuous backups, automated multi-region replication, in-memory caching, and data export tools. It is used to design and create Develop software applications, Create media metadata stores, and Deliver seamless retail experiences.

YELP API SERVICES
The Yelp Fusion API allows you to get the best local content and user reviews from millions of businesses across 32 countries. This tutorial provides an overview of the capabilities our suite of APIs offer, provides instructions for how to authenticate API calls, and walks through a simple scenario using the API.

▫️ YELP AUTHENTICATION:
The Yelp Fusion API uses private key authentication to authenticate all endpoints. Your private API Key will be automatically generated after you create your app.

# WORKFLOW DIAGRAM

![Screen Shot 2022-05-09 at 10 34 16](https://user-images.githubusercontent.com/86052891/167433391-b19a6f1e-f9dc-40db-8d3d-936c91be6996.png)

