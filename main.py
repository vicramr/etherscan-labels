from selenium import webdriver
import pandas as pd
import json
import time
import os.path
import pdb

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

        The other issue is subcats. Easy, not-totally-robust solution: just get the pages for both
        subcatid=1 and subcatid=0 because these are the most common. If they return the same
        addresses, only keep one of them.
        TODO 10/17, found out that in practice, this is NOT sufficient to get all the important ones
        (0x does not conform). Not fixing it now, but need to soon.

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

        # 1 is main, 0 is others
        subcats = [1, 0] if label not in config['main_only_list'] else [1]
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
            biggest_index = None
            tables = pd.read_html(driver.page_source)
            for i, tab in enumerate(tables):
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
        if len(subcat_tables) == 2: # TODO future proof this for more labels?
            # Invariant: either the tabs Main and Others both exist, and we got two
            # disjoint sets of addresses; or one (or both) of those tabs is missing,
            # and we got 2 identical sets of addresses.
            # Here we check this invariant.
            table_main, table_others = subcat_tables
            addresses_main = set(table_main['Address'].tolist())
            addresses_others = set(table_others['Address'].tolist())
            disjoint = addresses_main.isdisjoint(addresses_others)
            identical = addresses_main == addresses_others
            assert disjoint or identical # They can both be true for the empty set
            if disjoint:
                df = pd.concat(subcat_tables)
            else:
                df = table_main
        else:
            assert len(subcat_tables) == 1
            df = subcat_tables[0]

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
    raise NotImplementedError('Not updated for new directory structure')
    combinedJSON = {}

    # iterating over all files
    for files in os.listdir('./data'):
        if files.endswith('json'):
            print(files)  # printing file name of desired extension
            with open('./data/{}'.format(files)) as f:
                dictData = json.load(f)
                for address, nameTag in dictData.items():
                    if address not in combinedJSON:
                        combinedJSON[address] = {'name':nameTag,'labels':[]}
                    combinedJSON[address]['labels'].append(files[:-5])
        else:
            continue

    with open('combined/combinedLabels.json', 'w', encoding='utf-8') as f:
        json.dump(combinedJSON, f, ensure_ascii=True)

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
