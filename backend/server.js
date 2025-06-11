const express = require('express');
const multer = require('multer');
const cors = require('cors');
const parquet = require('parquetjs-lite');
const path = require('path');
const fs = require('fs');

const app = express();
const upload = multer({ dest: 'uploads/' });

app.use(cors());
app.use(express.json());

app.post('/api/parquet', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: '파일이 업로드되지 않았습니다.' });
    }

    const reader = await parquet.ParquetReader.openFile(req.file.path);
    const cursor = reader.getCursor();
    const data = [];
    let record = null;

    // 모든 레코드를 읽어옵니다
    while (record = await cursor.next()) {
      data.push(record);
    }

    // 컬럼 정보를 가져옵니다
    const columns = reader.metadata.row_groups[0].columns.map(col => 
      col.meta_data.path_in_schema[0]
    );

    // 임시 파일 삭제
    fs.unlinkSync(req.file.path);

    res.json({
      columns,
      data
    });
  } catch (error) {
    console.error('Error processing parquet file:', error);
    res.status(500).json({ error: '파일 처리 중 오류가 발생했습니다.' });
  }
});

const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
}); 