============
Templating
============

Sometimes you want to generate a big chunk of text containing a lot of
variables.  For example, you might want to generate an XML file or a
HTML page.  The `Templating` class provides a simple way to create such
text.

Consider the following example::

        <?xml version="1.0" encoding="UTF-8"?>
        <person>
            <name>John Doe</name>
            <age>42</age>
        </person>


This is a simple XML file. If you wanted to generate this file using
content from e.g. a database where you could be receiving any number of
different names and ages, you would have to do a lot of string
manipulation to get the desired result.  The `Templating` class makes
this easy with the help of the library Jinja2.

First, create your template file and put it in a folder for templates::

        <?xml version="1.0" encoding="UTF-8"?>
        {% for person in persons -%}
        <person>
            <name>{{ name }}</name>
            <age>{{ age }}</age>
        </person>
        {% endfor -%}

The template file is a normal XML file, but it contains some special
tags that are used by Jinja2.  The ``{% for person in persons -%}`` tag
starts a loop that will iterate over all the persons in the ``persons``
list.  The ``{% endfor -%}`` tag ends the loop.  The ``{{ name }}`` and
``{{ age }}`` tags are replaced with the values of the ``name`` and
``age`` variables.

Using this in Python, you would do something like this (assuming you
have put the above template in a file called ``person.xml`` in a folder
called ``/tmp/templates``):

.. code-block:: python

        from pyprediktorutilities.templating import Templating

        template = Templating("/tmp/templates")
        persons = [{"name": "John Doe", "age": 42},
                   {"name": "Jane Doe", "age": 43}]
        xml = template.render("person.xml", persons=persons)
        print(xml)

This would return the following string::
    
            <?xml version="1.0" encoding="UTF-8"?>
            <person>
                <name>John Doe</name>
                <age>42</age>
            </person>
            <person>
                <name>Jane Doe</name>
                <age>43</age>
            </person>

The ``render`` method takes the name of the template file as the first
argument.  The second argument is a dictionary containing the variables.

If you want to output the result to a file instead of a string, you can
use the ``render_to_file`` method, which is similar to the ``render``
method, but takes a file path as the second argument after the template.
