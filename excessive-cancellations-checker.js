import fs from 'fs';
import csv from 'csv-parser';

/**
 * Reads the CSV file and returns the data as an array of objects.
 */
async function readCSV() {
    let results = [];

    return new Promise((resolve, reject) => {
        try {
            fs.createReadStream('./data/trades.csv')
                .pipe(csv([
                    'time',
                    'companyName',
                    'orderType',
                    'quantity',
                ]))
                .on('data', (data) => results.push(data))
                .on('end', () => {
                    // Mapping the quantity to a number 
                    results = results.map((result) => {
                        return {
                            ...result,
                            quantity: +result.quantity,
                        };
                    });
                    resolve(results);
                });
        } catch (error) {
            reject("Failed to read CSV file: ", error);
        }
    });
}

export class ExcessiveCancellationsChecker {

    // We are using a promise to store the companies data.
    companies;
    /* 
        We provide a path to a file when initiating the class
        you have to use it in your methods to solve the task
    */
    constructor(filePath) {
        this.filePath = filePath;

        this.companies = new Promise(async (resolve, reject) => {
            const companies = {};
            let results;

            try {
                results = await readCSV('./data/trades.csv');
            }
            catch (error) {
                reject("Failed to populate companies: ", error);
            }

            results.forEach((result) => {
                if (companies[result.companyName] === undefined) {
                    companies[result.companyName] = [result];
                } else {
                    companies[result.companyName].push(result);
                }
            });

            resolve(companies);
        });
    }

    /**
    * Returns the list of companies that are involved in excessive cancelling.
    * Note this should always resolve an array or throw error.
    */
    async companiesInvolvedInExcessiveCancellations() {
        let badCompanies  = [];
        Object.entries(await this.companies).forEach(([company, trades]) => {
            if (this.checkExcessiveCancellation(trades)) {
                badCompanies.push(company);
            }
        });

        return badCompanies;
    }

    /**
    * Returns the total number of companies that are not involved in any excessive cancelling.
    * Note this should always resolve a number or throw error.
    */
    async totalNumberOfWellBehavedCompanies() {
        let numberOfGoodCompanies = 0;

        Object.entries(await this.companies).forEach(([company, trades]) => {
            if (!this.checkExcessiveCancellation(trades)) {
                numberOfGoodCompanies++;
            }
        });

        return numberOfGoodCompanies;
    }

    /**
    * Returns true if a company is involved in excessive cancelling otherwise false. 
    */
    checkExcessiveCancellation(trades) {
        // If a company has only one trade and it is a cancel, 
        // then return true for excessive cancellation otherwise return false
        if (trades.length === 1) {
            if (trades[0].orderType === 'F') {
                return true;
            } else {
                return false;
            }
        }

        // We are using a sliding window approach to check if a company is involved in excessive cancelling.
        // We are using two pointers firstTradeIndex and lastTradeIndex to keep track of the window.
        // We are also keeping track of cumulativeOrdersQuantity and cumulativeCancelQuantity as we move the window.
        let firstTradeIndex = 0;
        let lastTradeIndex = 0;
        let cumulativeOrdersQuantity = 0;
        let cumulativeCancelQuantity = 0;

        if (trades[0].orderType === 'D') {
            cumulativeOrdersQuantity = trades[0].quantity;
        } else {
            cumulativeCancelQuantity = trades[0].quantity;
        }

        while (lastTradeIndex + 1 < trades.length) {

            // We are checking if the difference between the last trade and the first trade is greater than 60 seconds.
            if (new Date(trades[lastTradeIndex + 1].time) - new Date(trades[firstTradeIndex].time) > 60 * 1000) {

                // If the ratio is greater than 1/3, then we are returning true for excessive cancellation.
                if (cumulativeCancelQuantity / cumulativeOrdersQuantity > 1 / 3) {
                    return true;
                }

                // Otherwise, we are moving the window to the right by incrementing the lastTradeIndex.
                lastTradeIndex++;

                // We are updating the cumulativeOrdersQuantity and cumulativeCancelQuantity based on the last trade.
                if (trades[lastTradeIndex].orderType === 'D') {
                    cumulativeOrdersQuantity = cumulativeOrdersQuantity + trades[lastTradeIndex].quantity;
                } else {
                    cumulativeCancelQuantity = cumulativeCancelQuantity + trades[lastTradeIndex].quantity;
                }

                // We are updating the firstTradeIndex to the current index of the window (to be within 60 seconds).
                for (let currentIndex = firstTradeIndex; currentIndex < lastTradeIndex;) {
                    if (new Date(trades[lastTradeIndex].time) - new Date(trades[currentIndex].time) > 60 * 1000) {

                        if (trades[currentIndex].orderType === 'D') {
                            cumulativeOrdersQuantity = cumulativeOrdersQuantity - trades[currentIndex].quantity;
                        } else {
                            cumulativeCancelQuantity = cumulativeCancelQuantity - trades[currentIndex].quantity;
                        }

                        currentIndex++;
                    }
                    else {
                        firstTradeIndex = currentIndex;
                        break;
                    };
                }
            }

            // If the difference is less than 60 seconds,
            // then we are moving the window to the right by incrementing the lastTradeIndex.
            // We are updating the cumulativeOrdersQuantity and cumulativeCancelQuantity based on the last trade.
            else {
                lastTradeIndex++;

                if (trades[lastTradeIndex].orderType === 'D') {
                    cumulativeOrdersQuantity = cumulativeOrdersQuantity + trades[lastTradeIndex].quantity;

                } else {
                    cumulativeCancelQuantity = cumulativeCancelQuantity + trades[lastTradeIndex].quantity;
                }
            }
        }

        // Finally, we are checking if the ratio of cumulativeCancelQuantity to cumulativeOrdersQuantity is greater than 1/3 for the last window.
        if (cumulativeCancelQuantity / cumulativeOrdersQuantity > 1 / 3) {
            return true;
        }

        // If no excessive cancellation is found, then we are returning false.
        return false;
    }

}
