#!/usr/bin/env python

from flask_ncaa_mbb import app

app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
