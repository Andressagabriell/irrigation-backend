require('dotenv').config();
const mqtt       = require('mqtt');
const axios      = require('axios');
const express    = require('express');
const cors       = require('cors');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(cors());
app.use(express.json());

const MQTT_BROKER   = process.env.MQTT_BROKER;
const MQTT_TOPIC    = process.env.MQTT_TOPIC;
const ML_API_URL    = process.env.ML_API_URL;
const PORT          = process.env.PORT || 3002;
const SUPABASE_URL  = process.env.SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_KEY;

console.log('🔧 ML_API_URL:', ML_API_URL);

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

let latestData  = null;
let climateData = { rain_last_6h: 0, rain_next_6h: 0 };

async function fetchClimate() {
  try {
    const url = 'https://api.open-meteo.com/v1/forecast' +
      '?latitude=-21.7167&longitude=-46.8167' +
      '&hourly=precipitation' +
      '&forecast_days=1' +
      '&timezone=America%2FSao_Paulo';

    const response = await axios.get(url);
    const hourly   = response.data.hourly;
    const now      = new Date();
    const currentHour = now.getHours();

    const last6h = hourly.precipitation
      .slice(Math.max(0, currentHour - 6), currentHour)
      .reduce((a, b) => a + b, 0);

    const next6h = hourly.precipitation
      .slice(currentHour, currentHour + 6)
      .reduce((a, b) => a + b, 0);

    climateData = {
      rain_last_6h: parseFloat(last6h.toFixed(2)),
      rain_next_6h: parseFloat(next6h.toFixed(2))
    };

    console.log(`🌦️  Clima atualizado — Chuva últimas 6h: ${climateData.rain_last_6h}mm | Próximas 6h: ${climateData.rain_next_6h}mm`);
  } catch (err) {
    console.error('❌ Erro ao buscar clima:', err.message);
  }
}

fetchClimate();
setInterval(fetchClimate, 30 * 60 * 1000);

app.get('/api/latest', (req, res) => {
  if (!latestData) {
    return res.status(404).json({ error: 'Nenhum dado recebido ainda.' });
  }
  res.json(latestData);
});

app.get('/api/history', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('leituras')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(50);

    if (error) throw error;
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const client = mqtt.connect(MQTT_BROKER);

client.on('connect', () => {
  console.log('✅ Conectado ao MQTT broker!');
  client.subscribe(MQTT_TOPIC, (err) => {
    if (!err) console.log(`📡 Escutando tópico: ${MQTT_TOPIC}`);
  });
});

client.on('message', async (topic, message) => {
  try {
    const sensorData = JSON.parse(message.toString());

    const data = {
      ...sensorData,
      rain_last_6h: climateData.rain_last_6h,
      rain_next_6h: climateData.rain_next_6h
    };

    console.log('\n📥 Dados recebidos do ESP32:');
    console.log(`   🌱 Umidade do solo:    ${data.soil_moisture}%`);
    console.log(`   🌡️  Temperatura:        ${data.air_temperature}°C`);
    console.log(`   💧 Umidade do ar:      ${data.air_humidity}%`);
    console.log(`   🌧️  Chuva últimas 6h:  ${data.rain_last_6h}mm`);
    console.log(`   🌦️  Chuva próximas 6h: ${data.rain_next_6h}mm`);

    console.log('\n🤖 Consultando modelo de ML...');
    console.log('🔗 URL:', ML_API_URL);

    const mlResponse = await axios.post(ML_API_URL, data, { timeout: 30000 });
    const { decision, confidence } = mlResponse.data;

    console.log(`\n✅ Decisão do sistema:`);
    console.log(`   💡 Ação:      ${decision === 'irrigar' ? '🚿 IRRIGAR' : '⏸️  NÃO IRRIGAR'}`);
    console.log(`   📊 Confiança: ${(confidence * 100).toFixed(0)}%`);
    console.log('─'.repeat(40));

    latestData = {
      soil_moisture:   data.soil_moisture,
      air_temperature: data.air_temperature,
      air_humidity:    data.air_humidity,
      rain_last_6h:    data.rain_last_6h,
      rain_next_6h:    data.rain_next_6h,
      decision,
      confidence: parseFloat((confidence * 100).toFixed(0)),
      timestamp:  new Date().toISOString()
    };

    const { error } = await supabase
      .from('leituras')
      .insert([{
        soil_moisture:   data.soil_moisture,
        air_temperature: data.air_temperature,
        air_humidity:    data.air_humidity,
        rain_last_6h:    data.rain_last_6h,
        rain_next_6h:    data.rain_next_6h,
        decision,
        confidence: parseFloat((confidence * 100).toFixed(0))
      }]);

    if (error) {
      console.error('❌ Erro ao salvar no Supabase:', error.message);
    } else {
      console.log('💾 Dados salvos no Supabase!');
    }

    if (decision === 'irrigar') {
      client.publish('irrigacao/comando', 'irrigar');
      console.log('💧 Comando enviado: IRRIGAR');
    } else {
      client.publish('irrigacao/comando', 'cancelar');
      console.log('⏸️  Comando enviado: CANCELAR');
    }

  } catch (err) {
    console.error('❌ Erro completo:', err.message || JSON.stringify(err));
    console.error('❌ Codigo:', err.code);
    console.error('❌ Response status:', err.response?.status);
    console.error('❌ Response data:', JSON.stringify(err.response?.data));
  }
});

client.on('error', (err) => {
  console.error('❌ Erro no MQTT:', err.message);
});

app.listen(PORT, () => {
  console.log(`🌿 Sistema de irrigação inteligente iniciado!`);
  console.log(`🚀 API rodando em http://localhost:${PORT}`);
});