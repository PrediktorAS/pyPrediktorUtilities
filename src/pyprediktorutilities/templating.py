from jinja2 import Environment, FileSystemLoader, select_autoescape, StrictUndefined
from pyprediktorutilities.shared import validate_folder, validate_file
import logging
import os

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Templating:
    """Simple wrapper around a templating engine, allowing for easy rendering of templates
    such as XMLs and HTMLs. Build on top of Jinja2
    
    Args:
        path (str): The path to the folder containing the templates
    
    Attributes:
        path (str): The path to the folder containing the templates
        env (jinja2.Environment): The environment to use for rendering
    
    Raises:
        FileNotFoundError: If the folder does not exist
        FileNotFoundError: If the template does not exist
        Exception: If the template could not be rendered
    
    Examples:
        >>> from pyprediktorutilities.templating import Templating
        >>> templating = Templating('templates')
        >>> templates = templating.list_templates()
        >>> template = templating.load_template('base.html')
        >>> rendered_template = templating.render(template, title='Home')
        >>> templating.render_to_file(template, 'output.html', title='Home')
    """
    
    def __init__(self, path: str) -> object:
        """Class initialization

        Args:
            path (str): The path to the folder containing the templates

        Raises:
            FileNotFoundError: If the folder does not exist

        Returns:
            object: Templating object
        """
        self.path = path
        # Check if folder exists
        try:
            validate_folder(path)
        except FileNotFoundError:
            errormsg = f"Folder {path} does not exist"
            logging.error(errormsg)
            raise FileNotFoundError(errormsg)
        
        # Establish the environment
        self.env = Environment(
            loader=FileSystemLoader(path), autoescape=select_autoescape(), undefined=StrictUndefined
        )
        
    def list_templates(self) -> list[str]:
        """Returns a list of templates in the folder

        Returns:
            list[str]: A list of strings containing the template names
        """
        return self.env.list_templates()

    def load_template(self, template: str) -> object:
        """Loads a template from the folder

        Args:
            template (str): The file name of the template to load

        Raises:
            FileNotFoundError: If the template does not exist

        Returns:
            object: The template object
        """
        # Check if template exists
        try:
            validate_file(os.path.join(self.path, template))
        except FileNotFoundError:
            errormsg = f"Template {template} not found in {self.path}"
            logging.error(errormsg)
            raise FileNotFoundError(errormsg)

        return self.env.get_template(template)

    def render(self, template: str, **kwargs) -> str:
        """Render a template with the given arguments and return a string

        Args:
            template (str): The file name of the template to load
            **kwargs: The arguments to pass to the template
            
        Raises:
            Exception: If the template could not be rendered

        Returns:
            str: The rendered template
        """
        template = self.env.get_template(template)
        try:
            return template.render(**kwargs)
        except Exception as e:
            errormsg = f"Could not render {template}: {e}"
            logging.error(errormsg)
            raise Exception(errormsg)
    
    def render_to_file(self, template: str, file: str, **kwargs) -> None:
        """Render a template with the given arguments and write to a file

        Args:
            template (str): The file name of the template to load
            file (str): The path and file name of the file to write to
            **kwargs: The arguments to pass to the template
        
        Raises:
            Exception: If the template could not be rendered
            FileNotFoundError: If the template does not exist
        
        Returns:
            None: None            
        """
        with open(file, "w") as f:
            f.write(self.render(template, **kwargs))
