require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const movieRoutes = require('../routes/movieRoutes');
const redis = require('../config/redis');

const app = express();
app.use(bodyParser.json());
app.use('/', movieRoutes);

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => console.log(`Eventura Autocomplete running on port ${PORT}`));