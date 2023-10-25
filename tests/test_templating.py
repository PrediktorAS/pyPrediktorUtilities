import os
import tempfile
from unittest import TestCase, mock
from pyprediktorutilities.templating import Templating


class TestTemplating(TestCase):
    def setUp(self):
        self.templates_dir = tempfile.TemporaryDirectory()
        self.templating = Templating(self.templates_dir.name)

    def tearDown(self):
        self.templates_dir.cleanup()

    def test_initilization(self):
        with self.assertRaises(FileNotFoundError):
            nonexist = Templating('nonexistent_folder')

    def test_list_templates(self):
        templates = self.templating.list_templates()
        self.assertEqual(templates, [])

        with open(os.path.join(self.templates_dir.name, 'template1.html'), 'w') as f:
            f.write('<html><body>{{ title }}</body></html>')

        with open(os.path.join(self.templates_dir.name, 'template2.xml'), 'w') as f:
            f.write('<root><title>{{ title }}</title></root>')

        templates = self.templating.list_templates()
        self.assertEqual(set(templates), {'template1.html', 'template2.xml'})

    def test_load_template(self):
        with open(os.path.join(self.templates_dir.name, 'template1.html'), 'w') as f:
            f.write('<html><body>{{ title }}</body></html>')

        template = self.templating.load_template('template1.html')
        self.assertIsNotNone(template)

        with self.assertRaises(FileNotFoundError):
            self.templating.load_template('nonexistent_template.html')

    def test_render(self):
        with open(os.path.join(self.templates_dir.name, 'template1.html'), 'w') as f:
            f.write('<html><body>{{ title }}</body></html>')

        template = self.templating.load_template('template1.html')
        rendered_template = self.templating.render(template, title='Home')
        self.assertEqual(rendered_template, '<html><body>Home</body></html>')

    def test_render_missing_arg(self):
        string = """
        <html><body>
        {% for i in does_not_exist -%}
        {{ i }}
        {% endfor -%}
        </body></html>
        """
        with open(os.path.join(self.templates_dir.name, 'template1.html'), 'w') as f:  
            f.write(string)

        template = self.templating.load_template('template1.html')
        with self.assertRaises(Exception):
            self.templating.render(template)

    def test_render_to_file(self):
        with open(os.path.join(self.templates_dir.name, 'template1.html'), 'w') as f:
            f.write('<html><body>{{ title }}</body></html>')

        template = self.templating.load_template('template1.html')

        with tempfile.NamedTemporaryFile(delete=False) as f:
            self.templating.render_to_file(template, f.name, title='Home')
            with open(f.name, 'r') as rendered_file:
                self.assertEqual(rendered_file.read(), '<html><body>Home</body></html>')
