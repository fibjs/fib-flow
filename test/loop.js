const child_process = require('child_process');
const path = require('path');

const testFiles = path.join(__dirname, 'index.js');

for (var i = 0; i < 100; i++) {
    const result = child_process.run(process.execPath, [testFiles, '--memory']);
    if (result.error) {
        console.error(`Error running test: ${result.error.message}`);
        break;
    }
}
