import concurrent.futures
import os
import time
import requests
import re

import selenium.common.exceptions
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
import multiprocessing

# edge_driver = 'C:\\Users\\Maru\\PycharmProjects\\venv\\Lib\\site-packages\\selenium\\webdriver\\edge'
chrome_driver = 'C:\\Users\\Maru\\PycharmProjects\\venv\\Lib\\site-packages\\selenium\\webdriver\\chrome'
# os.environ['PATH'] += edge_driver
os.environ['PATH'] += chrome_driver
web_links = {'digital archive': 'https://digital.nmla.metoffice.gov.uk/SO_1118bfbb-f2c9-476f-aa07-eb58b6db5ce6/', }


def bot(urls, year_tags):
    driver = webdriver.Chrome()
    actions = ActionChains(driver)
    try:
        print(f'FIRST STAGE INITIATED FOR: {year_tags}....')
        driver.get(web_links.get('digital archive'))

        first_element = driver.find_element(By.CSS_SELECTOR, f'a[href="{urls}"]')
        driver.execute_script("arguments[0].scrollIntoView();", first_element)
        # actions.move_to_element(first_element)
        first_element.click()

        print(f'FIRST STAGE SUCCESSFUL FOR: {year_tags}....')
        print(f'SECOND STAGE INITIATED for {year_tags}....')
        sTWO_url = driver.current_url
        sTWO_site = requests.get(sTWO_url)
        sTWO_web_objects = BeautifulSoup(sTWO_site.text, 'lxml')
        monthly_placeholders = sTWO_web_objects.find(name='div', attrs={'class': 'twelve columns last results'})
        months = monthly_placeholders.find_all(name='h5')

        driver.find_element(By.XPATH, '//*[@id="cookie-consent-banner"]/div/button').click() # remove cookie pop-up

        for month_href_tags in months:
            month_tags = f'{month_href_tags.get_text()}'
            month_hrefs = re.findall(regex, str(month_href_tags))
            for href in month_hrefs:
                try:
                    print(f'REGEX: MONTH: {month_tags}: CLICKING: a[href="{href}"]')  # debug months found by REGEX

                    second_element = driver.find_element(By.CSS_SELECTOR, f'a[href="{href}/"]')
                    driver.execute_script("arguments[0].scrollIntoView();", second_element)
                    # actions.move_to_element(second_element)
                    second_element.click()

                    print(f'SECOND STAGE SUCCESSFUL FOR: {year_tags}....')
                    print(f'THIRD STAGE INITIATED for: Month Title: {month_tags})')
                    sTWO_url = driver.current_url
                    download_site = requests.get(sTWO_url)
                    content = BeautifulSoup(download_site.text, 'lxml')
                    nav_controls = content.find_all(name='a', attrs={'class':'new-primary new-primary-tint-hover fa fa-download'})
                    download_regex = r'(?<=href=\").{1,}(?=\" title)'

                    for button in nav_controls:  # debug button
                        print(F'BS4: BUTTON ELEMENT, FINDING AND SUBSTITUTING: {button}')  # debug button element found by BS4
                        button_hrefs = re.findall(download_regex, str(button))

                        for downl_button in button_hrefs:
                            try:
                                print(f'REGEX: DOWNLOAD ELEMENT, CLICKING: a[href="{downl_button}"]')
                                driver_element = driver.find_element(By.CSS_SELECTOR, f'a[href="{downl_button}"]')
                                driver_element.click()
                                time.sleep(.5)
                                print(f'THIRD STAGE SUCCESSFUL FOR: {year_tags}:{month_tags}....')
                                driver.back()

                            except Exception as e:
                                print('THIRD Stage Failed: Got unexpected exception:', e)
                                break

                except selenium.common.exceptions as e:
                    print('SECOND Stage Failed:', e)
                    break
    except selenium.common.exceptions as e:
        print('FIRST Stage Failed:', e)


if __name__ == '__main__':
    sONE_url = requests.get(web_links.get('digital archive'))
    sONE_web_objects = BeautifulSoup(sONE_url.text, 'lxml')

    year_placeholder = sONE_web_objects.find(name='div', attrs={'class': 'sixteen columns results-and-filters'})
    years = year_placeholder.find_all(name='div', attrs={'class': ['one_sixth grey_block new-secondary-background result-item',
                                                                   'one_sixth grey_block new-secondary-background result-item last']})  # don't skip, needed for titles.
    unit = [years.find('h5') for years in years]
    regex = r'(?<=href=\").{1,}(?=\/")'  # lookaround = PositiveLookBehind...PositiveLookAhead
    year_css_selector = {}
    titles = [years.get('title') for years in years]

    for year_href_tags, year_tag in zip(unit, titles):  # href_tag -> bs4 component
        hrefs = re.findall(regex, str(year_href_tags.get_text))  # href_tag.get_text -> method that enables str.

        for year_href in hrefs:
            year_css_selector.update({f'{year_tag}': f'{year_href}/'})

    for year_t, year_h in zip(year_css_selector.keys(), year_css_selector.values()):
        bot(year_h, year_t)
