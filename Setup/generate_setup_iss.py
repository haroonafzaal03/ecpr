import os
import yaml
import jinja2
import platform


CUSTOMERS_FILE = 'customers.yaml'

ISS_TEMPLATE_FILE = 'templates/ECPRSetup.iss.j2'
OUTPUT_DIR = 'output/ECPRSetup_{}_{}.iss'

ISS_UPDATE_TEMPLATE_FILE = 'templates/ECPRUpdate.iss.j2'
OUTPUT_UPDATE_DIR = 'output/ECPRUpdate_{}_{}.iss'

BUILD_TEMPLATE_FILE = 'templates/build.bat.j2'
BUILD_BASH_TEMPLATE_FILE = 'templates/build.sh.j2'

AFTER_INSTALL_TEMPLATE_FILE = 'templates/after_install.bat.j2'
AFTER_UPDATE_TEMPLATE_FILE = 'templates/after_update.bat.j2'
BEFORE_UNINSTALL_TEMPLATE_FILE = 'templates/before_uninstall.bat.j2'
README_FILE = 'templates/README.txt.j2'

VERSION_FILE = '../version.txt'
CONFIGURATION_FILE = '../configuration.txt'

AWS_S2_ACCESS_KEY_ID = os.environ.get('AWS_S2_ACCESS_KEY_ID', "access_key_id")
AWS_S2_SECRET_ACCESS_KEY = os.environ.get('AWS_S2_SECRET_ACCESS_KEY', "secret_access_key")

BUILD_CONFIGURATION = os.environ.get('CONFIGURATION', 'debug')

JINJA_ENVIRONMENT = jinja2.Environment(
    comment_start_string="!#",
    comment_end_string="#!",
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)


def generate_update_iss(customers_spec):
    template = JINJA_ENVIRONMENT.get_template(ISS_UPDATE_TEMPLATE_FILE)
    for cs in customers_spec:
        cs.update(dict(os_type=platform.system()))
        with open(OUTPUT_UPDATE_DIR.format(cs['customer_code'], cs['release_for']), 'w') as f:
            f.write(template.render(cs))
            f.close()


def generate_setup_iss(customers_spec):
    template = JINJA_ENVIRONMENT.get_template(ISS_TEMPLATE_FILE)
    for cs in customers_spec:
        cs.update(dict(os_type=platform.system()))
        with open(OUTPUT_DIR.format(cs['customer_code'], cs['release_for']), 'w') as f:
            f.write(template.render(cs))
            f.close()


def generate_build_batch(customers_spec, app_configuration):
    template = JINJA_ENVIRONMENT.get_template(BUILD_TEMPLATE_FILE)
    with open('build.bat', 'w') as f:
        f.write(template.render(customers_spec=customers_spec, app_configuration=app_configuration))
        f.close()


def make_executable(path):
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2
    os.chmod(path, mode)


def generate_build_bash(customers_spec):
    template = JINJA_ENVIRONMENT.get_template(BUILD_BASH_TEMPLATE_FILE)
    with open('build.sh', 'w') as f:
        f.write(template.render(customers_spec=customers_spec))
        f.close()
    make_executable('build.sh')


def generate_post_push_script(customers_spec):
    try:
        os.mkdir('scripts')
    except:
        pass

    for tpl in [AFTER_INSTALL_TEMPLATE_FILE, AFTER_UPDATE_TEMPLATE_FILE, BEFORE_UNINSTALL_TEMPLATE_FILE, README_FILE]:
        template = JINJA_ENVIRONMENT.get_template(tpl)
        for cs in customers_spec:
            try:
                os.mkdir('scripts/{}'.format(cs['customer_code']))
            except:
                pass
            try:
                os.mkdir('scripts/{}/{}'.format(cs['customer_code'], cs['release_for']))
            except:
                pass
            with open('scripts/{}/{}/{}'.format(cs['customer_code'], cs['release_for'], tpl[:-3].split('/')[1]), 'w') as f:
                f.write(template.render(cs))
                f.close()


def get_version():
    v = None
    with open(VERSION_FILE, 'r') as f:
        v = f.read()
        f.close()
    return v


def get_configuration():
    """
    v = None
    with open(CONFIGURATION_FILE, 'r') as f:
        v = f.read()
        f.close()
    return v
    """
    return BUILD_CONFIGURATION.lower()


def main():
    app_configuration = get_configuration()
    app_version = get_version()
    customers_spec = None
    with open(CUSTOMERS_FILE) as f:
        customers_spec = yaml.load(f)
        f.close()

    for cs in customers_spec:
        cs['app_version'] = app_version.strip('\n')
        cs['AWS_S2_ACCESS_KEY_ID'] = AWS_S2_ACCESS_KEY_ID
        cs['AWS_S2_SECRET_ACCESS_KEY'] = AWS_S2_SECRET_ACCESS_KEY

    generate_setup_iss(customers_spec)
    generate_update_iss(customers_spec)
    generate_build_batch(customers_spec, app_configuration)
    generate_post_push_script(customers_spec)

main()
