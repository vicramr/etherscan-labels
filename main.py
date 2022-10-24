from selenium import webdriver
import pandas as pd
import json
import time
import os.path
import sys

ETHERSCAN_BASEURL = 'https://etherscan.io/'
OPTIMISTIC_BASEURL = 'https://optimistic.etherscan.io/'
ARBISCAN_BASEURL = 'https://arbiscan.io/'
POLYGONSCAN_BASEURL = 'https://polygonscan.com/'
GNOSISSCAN_BASEURL = 'https://gnosisscan.io/'

CHAIN_MAP = {
    'ethereum': ETHERSCAN_BASEURL,
    'optimism': OPTIMISTIC_BASEURL,
    'arbitrum': ARBISCAN_BASEURL,
    'polygon': POLYGONSCAN_BASEURL,
    'gnosis': GNOSISSCAN_BASEURL
}

IGNORE_LIST_MAP = {
    # Large size: Eth2/gnsos , safe-multisig , beacon-depositor , contract-deployer , Bugged: Liqui , NoData: Remaining labels
    'ethereum': ['eth2-depositor', 'gnosis-safe-multisig', 'safe-multisig', 'beacon-depositor', 'contract-deployer', 'liqui.io','education','electronics','flashbots','media','music','network','prediction-market','real-estate','vpn'],
    'optimism': [],
    'arbitrum': [],
    'polygon': ['contract-deployer'], # Too big + not interesting
    'gnosis': []
}

URL_TYPE_MAP = {
    'ethereum': 'complex',
    'optimism': 'complex',
    'arbitrum': 'simple',
    'polygon': 'simple',
    'gnosis': 'simple'
}

# Too big for Others, but ok for Main (only relevant for etherscan.io)
# This should only be used at all for the Etherscan-like explorers
MAIN_ONLY_MAP = {
    'ethereum': ['token-contract', 'uniswap'],
    'optimism': [],
    'arbitrum': None,
    'polygon': None,
    'gnosis': None
}


# All subcat IDs found from etherscan; determined manually.
ETHERSCAN_SUBCAT_IDS = [
    '1', # Main for most labels
    '0', # Others for most labels
    '3-0', # Others for some (e.g. 0x)
    '2', # Legacy for some labels
    '3', # Others for coinhako and some others
]



# Login to etherscan and auto fill login information if available
def login():
    driver.get(os.path.join(config['baseurl'], 'login'))
    driver.implicitly_wait(5)
    driver.find_element_by_id(
        "ContentPlaceHolder1_txtUserName").send_keys(config['user'])
    driver.find_element_by_id(
        "ContentPlaceHolder1_txtPassword").send_keys(config['pass'])

    input("Press enter once logged in")
# https://optimistic.etherscan.io/accounts/label/aave?subcatid=undefined&size=25&start=25&col=1&order=asc

# Retrieve label information and saves as JSON/CSV
def getLabel(label, type='single'):
    """
    complex version:
        A few problems with etherscan that aren't present in other explorers (not even optimistic):
        1. order=asc apparently means nothing, because every query seems to just return a
            random sample of addresses.
        2. subcat actually means something... for some, but not all, labels (only those with tabs).
            The subcats are usually Main (with subcatid=1) and Others (with subcatid=0) but at least
            one label has different ones (binance).
            If the label does not have tabs, or an invalid subcatid is given, etherscan will return the
            default subcat (or, if there's only one, that one).

        Luckily, etherscan (and optimistic) will respond properly to an arbitrary value of the size
        parameter, so we can simply get ALL the addresses in a single query. If the given size is too
        large, it's fine; all the addresses will still be returned, and the page won't take any longer
        to load.

        The other issue is subcats. Manually determined that there are 5 subcatid values across the
        labels we're interested in (see ETHERSCAN_SUBCAT_IDS). By fetching the pages for all 5 of these
        and removing duplicate tables, we can make sure we get all of the addresses.
        Also note: in practice, the addresses in the returned tables are unique and they are disjoint
        between two different tabs for the same label. We assert this when scraping the addresses.

        Optimistic is more well-behaved (no tabs from manual inspection, and also order=asc works)
        but the same code should work properly for it.

        These two sites always add col=1 to their URLs. I can't tell what (if anything) this does.

    simple version:
        This is much easier. These explorers don't have tabs as far as I can see. We just pass a
        page index, and when we go past the end, we'll get an empty table with an error message.
    """
    print('Getting addresses for label', label)
    if config['url_type'] == 'complex':
        # TODO are we assuming that tables will be returned properly (i.e. not empty)? We might.
        labelUrl = os.path.join(
            config['baseurl'],
            'accounts/label/{}?subcatid={}&size={}&start=0&col=1&order=asc'
        )
        size = 7000 # TODO this is a quick and dirty solution, and in the future I'm sure there'll be larger ones

        subcats = ETHERSCAN_SUBCAT_IDS if label not in config['main_only_list'] else [1]
        # TODO for optimistic, just use one subcat
        subcat_tables = [] # list of tables, one per subcat
        for subcatid in subcats:
            formattedUrl = labelUrl.format(label, subcatid, size)
            driver.get(formattedUrl)
            driver.implicitly_wait(10)
            # not including the importerror

            # Find the biggest returned table. Under some conditions,
            # etherscan.io (not the others as far as I can tell) will
            # return multiple tables and the desired one won't be at index 0.
            max_size = 0
            biggest_index = 0
            tables = pd.read_html(driver.page_source)
            for i, tab in enumerate(tables):
                if (tab == 'No matching records found').sum().sum() > 1:
                    # Indicates that the table contains no useful info
                    # Need to filter these out because they could be the
                    # same size as a legit table with only 1 address
                    continue
                tabsize = len(tab.index)
                if tabsize > max_size:
                    max_size = tabsize
                    biggest_index = i
            newTable = tables[biggest_index]
            assert newTable['Name Tag'].iloc[-1].startswith('Sum of')
            newTableTruncated = newTable[:-1] # Remove last item which is just sum
            newTableTruncated.fillna('', inplace=True)
            subcat_tables.append(newTableTruncated) 
            print('Subcat', subcatid, 'has', len(newTableTruncated.index), 'addresses')

        assert len(subcat_tables) > 0
        for i, table in enumerate(subcat_tables):
            # Invariant: any two tables are either totally disjoint or totally equivalent.
            # This is because either a given subcatid exists, and it has a unique table, or
            # it doesn't exist, and it is the same as a previous table
            #
            # Also: each table's rows should be unique
            curr_addresses_list = table['Address'].tolist()
            curr_addresses_set = set(curr_addresses_list)
            assert len(curr_addresses_list) == len(curr_addresses_set)

            if i == 0:
                addresses_set = curr_addresses_set
                df = table
            else:
                superset = addresses_set.issuperset(curr_addresses_set)
                disjoint = addresses_set.isdisjoint(curr_addresses_set)
                assert superset or disjoint # They can both be true for the empty set
                if disjoint:
                    df = pd.concat([df, table])

        skip = False # Relevant below
        # The values used from this branch are df and skip
    else:
        assert config['url_type'] == 'simple'

        labelUrl = os.path.join(config['baseurl'], 'accounts/label/{}/{}')
        index = 1  # Initialize start index at 1
        table_list = []
        while (True):
            # pdb.set_trace()
            print('Index:', index)
            driver.get(labelUrl.format(label, index))
            driver.implicitly_wait(5)
            try:
                newTable = pd.read_html(driver.page_source)[0]
            except ImportError: # TODO this is just due to lxml right? Remove this and add dep.
                print(label, "Skipping label due to error")
                return
            # Check for ending condition
            if (newTable == 'There are no matching entries').sum().sum() > 1:
                # This message is returned at the first index beyond the end.
                # This html table should contain zero useful information.
                assert len(newTable) == 3 # Message row, null row, and sum row
                break

            assert newTable['Name Tag'].iloc[-1].startswith('Sum of')
            table_list.append(newTable[:-1])  # Remove last item which is just sum
            index += 1

        if len(table_list) == 0:
            print(label, 'has 0 addresses; skipping')
            skip = True
        else:
            skip = False
            df = pd.concat(table_list)  # Combine all dataframes
            df.fillna('', inplace=True)  # Replace NaN as empty string

            # Prints length
            print(label, 'Df length:', len(df.index))

    if not skip: # save as csv + json
        # Check that the addresses are unique; sanity check to ensure nothing went wrong
        n_rows = len(df.index)
        addresses = df['Address'].tolist()
        addresses_set = set(addresses)
        assert n_rows == len(addresses) == len(addresses_set)

        df.to_csv(os.path.join('data', config['chain'], '{}.csv'.format(label)))

        # Save as json object with mapping address:nameTag
        addressNameDict = dict([(address, nameTag)
                            for address, nameTag in zip(df.Address, df['Name Tag'])])
        with open(os.path.join('data', config['chain'], '{}.json'.format(label)), 'w', encoding='utf-8') as f:
            json.dump(addressNameDict, f, ensure_ascii=True)

    if (type == 'single'):
        endOrContinue = input(
            'Type "exit" end to end or "label" of interest to continue')
        if (endOrContinue == 'exit'):
            driver.close()
        else:
            getLabel(endOrContinue)

# Combines all JSON into a single file combinedLabels.json
def combineAllJson():
    combinedJSONAllChains = {}

    for chain in CHAIN_MAP.keys():
        print('Combine files for', chain)
        combinedJSON = combineAllJsonForChain(chain)
        combinedJSONAllChains[chain] = combinedJSON

    with open('combined/combinedLabelsAllChains.json', 'w', encoding='utf-8') as f:
        json.dump(combinedJSONAllChains, f, ensure_ascii=True)


def combineAllJsonForChain(chain):
    # chain is the chain name to stitch JSON files for, e.g. 'arbitrum' or 'ethereum'
    combinedJSON = {}
    datadir = os.path.join('data', chain)

    # iterating over all files
    for files in os.listdir(datadir):
        if files.endswith('json'):
            print(files)  # printing file name of desired extension
            with open(os.path.join(datadir, files)) as f:
                dictData = json.load(f)
                for address, nameTag in dictData.items():
                    if address not in combinedJSON:
                        combinedJSON[address] = {'name':nameTag,'labels':[]}
                    combinedJSON[address]['labels'].append(files[:-5])
        else:
            continue

    with open('combined/combinedLabels{}.json'.format(chain), 'w', encoding='utf-8') as f:
        json.dump(combinedJSON, f, ensure_ascii=True)

    return combinedJSON

# Retrieves all labels from labelcloud and saves as JSON/CSV
def getAllLabels():
    driver.get(os.path.join(config['baseurl'], 'labelcloud'))
    driver.implicitly_wait(5)

    elems = driver.find_elements_by_xpath("//a[@href]")
    labels = []
    labelIndex = len(os.path.join(config['baseurl'], 'accounts', 'label')) + 1 # Add 1 to ensure there is no leading '/'
    for elem in elems:
        href = elem.get_attribute("href")
        if (href.startswith(os.path.join(config['baseurl'], 'accounts', 'label'))):
            labels.append(href[labelIndex:])

    print(labels)
    print('L:', len(labels))

    for label in labels:
        if (os.path.exists(os.path.join('data', config['chain'], '{}.json'.format(label)))):
            print(label, 'already exists skipping.')
            continue
        elif label in ignore_list:
            print(label, 'ignored due to large size and irrelevance')
            continue
        getLabel(label, 'all')
        time.sleep(5)  # Give 5s interval to prevent RL

    # Proceed to combine all addresses into single JSON after retrieving all.
    # combineAllJson() # TODO update to work with multiple chains


with open('config.json', 'r') as f:
    config = json.load(f)
driver = webdriver.Chrome()

chain = input('Enter chain (ethereum/optimism/arbitrum/polygon/gnosis)')
if chain not in CHAIN_MAP:
    print('Non-chain option given. Combining JSON files and exiting.')
    combineAllJson()
    sys.exit(0)

config['baseurl'] = CHAIN_MAP[chain] # TODO handle bad inputs
config['chain'] = chain
config['url_type'] = URL_TYPE_MAP[chain]
config['main_only_list'] = MAIN_ONLY_MAP[chain]

global ignore_list
ignore_list = IGNORE_LIST_MAP[chain]

if chain == 'ethereum':
    config['user'] = config['ETHERSCAN_USER']
    config['pass'] = config['ETHERSCAN_PASS']
elif chain == 'optimism':
    config['user'] = config['OPTIMISTIC_USER']
    config['pass'] = config['OPTIMISTIC_PASS']
elif chain == 'arbitrum':
    config['user'] = config['ARBISCAN_USER']
    config['pass'] = config['ARBISCAN_PASS']
elif chain == 'polygon':
    config['user'] = config['POLYGONSCAN_USER']
    config['pass'] = config['POLYGONSCAN_PASS']
else:
    assert chain == 'gnosis'
    config['user'] = config['GNOSISSCAN_USER']
    config['pass'] = config['GNOSISSCAN_PASS']

login()
retrievalType = input('Enter retrieval type (single/all): ')
if (retrievalType == 'all'):
    getAllLabels()
else:
    singleLabel = input('Enter label of interest: ')
    getLabel(singleLabel)
