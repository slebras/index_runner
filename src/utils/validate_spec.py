import yaml

with open('spec/config.yaml') as fd:
    yaml.safe_load(fd)
    print('YAML successfully parsed')
