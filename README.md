# Aeron Messaging (transport)

[Short description]

This project was built on top of the awesome Aeron guide created by Mark Raynsford, which you can find here: <br>
http://www.io7m.com/documents/aeron-guide/

For more details see my fork (with a bit more explanations/details) of Mark's project here:
https://github.com/shevkoplyas/aeron-guide

<p>
"Aeron Messaging" allows us to create client and server instances and start exchanging messages via two bi-directional
channels. One is "public" channel, which server might use to send a message to all connected clients (think of it as
a "broadcast" channel) and the other is "private" channel (one per each connected client).
Aeron uses UDP datagrams and quite a few ports (read more about it in Mark's tutorial linked above).
Aeron client can connect to the server end establish bi-directional "connection" even when the client initiate the 
connection to the server from behind the NAT (say from yourhome networks).
</p>

<p>
Aeron is awesome and used in other project(s)  (todo: give a link here) to create one of many other types of transports
to support message exchange with remote clients. Just to give an idea: locally connected instances don't need to use
UDP protocol to communicate between each other, instead they'd use "local transport" and deliver messages by directly
injecting them into some sort of the thread-safe queues. Then remote agents would use "aeront transport" which will
use this project "Aeron Messaging" client/server to make it possible for the "aeron transport" to move messages from/to
connected client(s) and there can be other transports all working together in parallel for different other needs
(for example "http transport" or "file transport", where messages can be sent/received by means of using regular files,
which seems ridiculous, but can work really fast if files are mapped to memory and this can be the easiest choice to
connect some 3rd party software into the common messaging exchange).
</p>

To use this project:
  - download the sources
  - build it locally
    this will install compiled jar file into your local maven repository, see last build lines should be like this:
    Installing aeronmessaging/pom.xml to /home/username/.m2/repository/ca/dimon/aeronmessaging/1.0.1-SNAPSHOT/aeronmessaging-1.0.1-SNAPSHOT.pom
  - add aeronmessaging as a dependency in other project
  - start using it (todo: give short code snipped of instantiating client and server here)
