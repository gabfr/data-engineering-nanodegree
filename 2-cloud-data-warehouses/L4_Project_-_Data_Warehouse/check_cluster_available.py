from create_cluster import config_parse_file, aws_client, check_cluster_creation

def main():
    config_parse_file()

    redshift = aws_client('redshift', "us-east-2")

    if check_cluster_creation(redshift):
        print('available')
    else:
        print('notyet')

if __name__ == '__main__':
    main()