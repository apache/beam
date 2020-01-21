<pre>
This is being released by Medsea Business Solutions S.L. under the Apache Licence V2.

For Usage instructions see the web site http://www.medsea.eu/mime-util and the unit tests provided with this project.


MimeUtil Mime Type Detection utility is a very light weight utility that depends only on apache commons-logging.
It requires at least Java 1.4 as it uses the java.nio library and regular expressions

This document explains how mime types are detected and reported by this utility.

A MIME or "Multipurpose Internet Mail Extension" type is an Internet standard that is important outside of just e-mail use.
MIME is used extensively in other communications protocols such as HTTP for web communications.
IANA "Internet Assigned Numbers Authority" is responsible for the standardisation and publication of MIME types. Basically any
resource on any computer that can be located via a URI can be assigned a MIME type. So for instance, JPEG images have a MIME type
of image/jpg. Some resources can have multiple MIME types associated with them such as files with an XML extension have the MIME types
text/xml and application/xml and even specialised versions of xml such as image/svg+xml for SVG image files. The list of MIME types is quite
extensive but if you should require a new MIME type you can always specify your own. Obviously if you transmit this information specifying
your own MIME types, others around the world would not know what the information should represent, but you would be able to create clients
and applications that handle resources with these specific MIME type in-house no problem at all.

Anyway, MIME types have been around on the Internet nearly as long as the Internet itself and because of the length of time many
MIME types have been standardised such that applications can be written to understand and handle information transmitted over the
web in specific standardised ways. For instance HTML pages are transmitted with a mime type of text/html and browsers know what
to do with this type of information.

Mostly MIME types for files can be assumed based on their file extension such as files ending in .jar are assigned the MIME type
application/java-archive but some files do not have extensions such as Make files and this association is then not possible.
In these cases an alternative to MIME type association by file extension is needed. Luckily within the Unix world, file type
detection using magic numbers has been around for quite some time, even before the Internet. This technique peaks inside files to
known offsets and compares values. It does this based on a set of rules contained in a file called 'magic' located on the Unix file system.
The Unix 'file' command uses this file, or a compiled version of it, to determine information about the file. It recursively passes
the file through the rules defined in the 'magic' file until a match is found, or not. However, the 'magic' file itself does not contain
MIME type information so Unix again comes to the rescue with an alternative file called 'magic.mime' which has the exact same rule set as the
'magic' file but can guess the MIME types of the files based on the magic rule set. The down side of this detection method is that
it's much slower than association by file extension. Other alternatives include the Free Desktop "shared MIME database" and the Windows registry.
MIME type detection is not guaranteed to be accurate. It is a best guess approach where speed should be of greater consideration than accuracy.
This MIME utility several of the techniques described above.

This utility uses MimeDetect(s) to retrieve collections of MIME types. New MimeDetector(s) can be created by third parties to add to the detection
process. The utility comes with several pre-build MimeDetector(s), ExtensionMimeDetector, MagicMimeMimeDetector, OpendesktopMimeDetector and
TextMimeDetector.  Please see the relevant java doc for each of these MimeDetector(s) for a description of how each one works.

The Utility will execute each registered MimeDetector in turn accumulating the MIME types returned from each one. The resultant collection is then
returned to the client. The collection will contain only one entry for each MIME type and can never contain null. The collections returned are instances
of MimeTypeHashSet that has a backing LinkedHashSet object and implements all the methods of the java.util.Collection and java.util.Set interfaces.


There is a fairly comprehensive set of unit tests available for the project, so if you need a more in-depth explanation of how the utility works
check out the source code and unit tests.

TO CONTRIBUTE:::

Please register any bugs you find on SourceForge and we will do our best to address them ASAP.

It would also be very helpful for you to email in modifications to MIME types currently listed in the mime-types.properties file.
It would also be great if people would list any modifications or new rules to the MIME rules in the magic.mime types using the project forum.
Another way to contribute is to create new MimeDetector(s) and or TextMimeHandler(s), refer to the java doc and web site regarding implementation.
We expect that we will add these changes with each new release.

</pre>