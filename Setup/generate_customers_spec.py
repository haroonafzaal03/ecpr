import os
import yaml
import requests

ETCD_URL = 'https://{}.envoymobile.net/etcd/v2/keys/{}'

ANS_PROD = 'envpan'
ANS_DEV = 'envdan'

# ETCD_URL_DEV = 'http://cletan003/etcd/v2/keys'
# ETCD_URL = '{}/{}'
# ANS_DEV = ETCD_URL_DEV
# ANS_PROD = ETCD_URL_DEV

CUSTOMERS_SPEC_FILE = 'customers.yaml'

ECPR_CONFIG_URL = 'https://%s.envoymobile.net/config/{}/ecpr.json'
ECPR_KEYS_LENGTH = 6  # all keys have prefix == 'ecpr_'
ECPR_BASIC_AUTH_PASSWORD_KEY = 'ecpr_http_basic_auth_password'

ECPR_ENVOY_ALIAS_URL = ECPR_CONFIG_URL % ('envoy')
ECPR_RADIUS_ALIAS_URL = ECPR_CONFIG_URL % ('radius')

RELEASE_FOR_CODE = {
    'stable': 'p',
    'demo': 'm',
    'sandbox': 's',
    'beach': 'b',
    'testing': 't',
    'unstable': 'd'
    }


AUTH = ('envoy', os.environ['ETCD_ENVOY_PASSWORD'])


def get_customers_spec(srv=ANS_DEV):
    resp = requests.get(ETCD_URL.format(srv, 'customer'), auth=AUTH)

    if resp.status_code == 200:
        data = resp.json()['node']['nodes']

        cus_env = []
        cus_spec = []

        for d in data:
            customer_code = d['key'].split('/')[-1]
            resp = requests.get(ETCD_URL.format(srv, customer_code), auth=AUTH)

            if resp.status_code == 200:
                data_n = resp.json()['node']['nodes']

                for d_n in data_n:
                    cus_env.append(d_n['key'])

        for c_e in cus_env:
            resp = requests.get(ETCD_URL.format(srv, c_e), auth=AUTH)

            if resp.status_code == 200:
                node = resp.json()['node']

                if 'nodes' not in node:
                    continue

                data_v = node['nodes']

                ecpr_configured = False
                ecpr_key_counts = 0

                for d_v in data_v:
                    if 'dir' not in d_v or not d_v['dir']:
                        if 'ecpr_' in d_v['key']:
                            ecpr_key_counts += 1
                        if ecpr_key_counts >= ECPR_KEYS_LENGTH:
                            ecpr_configured = True

                if ecpr_configured:
                    ecpr_vars = {}

                    for d_v in data_v:
                        if 'ecpr_' in d_v['key']:
                            ecpr_vars[d_v['key'].split('/')[-1]] = d_v['value']

                    c_e_s = str(c_e).split('/')

                    ecpr_http_basic_auth_password = ecpr_vars['ecpr_http_basic_auth_password'].strip()
                    customer_code = c_e_s[1]
                    release_for = c_e_s[2]
                    release_for_code = RELEASE_FOR_CODE[release_for]

                    ccce = '{}{}'.format(customer_code, release_for_code)
                    config_url = ECPR_ENVOY_ALIAS_URL.format(ccce)

                    cus_spec.append({
                        'customer_code': customer_code,
                        'release_for': release_for,
                        'release_for_code': release_for_code,
                        'config_url': config_url,
                        'config_auth': {
                            'username': 'ecpr',
                            'password': str(ecpr_http_basic_auth_password)
                        }
                    })

    return cus_spec


def main():
    cus_spec = get_customers_spec(ANS_PROD) + get_customers_spec()

    if cus_spec == []:
        raise Exception('customers spec data is empty!!!')

    with open(CUSTOMERS_SPEC_FILE, 'w') as f:
        yaml.dump(cus_spec, f, default_flow_style=False)
        f.close()

if __name__ == '__main__':
    main()
