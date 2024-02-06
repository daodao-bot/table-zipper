-- init

DROP DATABASE IF EXISTS `zipper`;

CREATE DATABASE IF NOT EXISTS `zipper` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

SHOW CREATE DATABASE `zipper`;

USE `zipper`;

DROP TABLE IF EXISTS `zipper`.`cat`;

CREATE TABLE IF NOT EXISTS `zipper`.`cat`
(
    `id`          bigint unsigned                 NOT NULL AUTO_INCREMENT COMMENT 'id',
    `name`        varchar(16) COLLATE utf8mb4_bin NOT NULL COMMENT '名字',
    `create_time` datetime                        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime                        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `valid`       tinyint(1)                      NOT NULL DEFAULT '1' COMMENT '是否有效',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`),
    KEY `create_time` (`create_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin COMMENT ='猫';

DROP TABLE IF EXISTS `zipper`.`dog`;

CREATE TABLE IF NOT EXISTS `zipper`.`dog`
(
    `id`          bigint unsigned                 NOT NULL AUTO_INCREMENT COMMENT 'id',
    `name`        varchar(16) COLLATE utf8mb4_bin NOT NULL COMMENT '名字',
    `create_time` datetime                        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime                        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `valid`       tinyint(1)                      NOT NULL DEFAULT '1' COMMENT '是否有效',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`),
    KEY `create_time` (`create_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin COMMENT ='狗';

INSERT INTO `zipper`.`cat` (`name`)
VALUES ('喵-1');
INSERT INTO `zipper`.`cat` (`name`)
VALUES ('喵-2');

INSERT INTO `zipper`.`dog` (`name`)
VALUES ('汪-1');
INSERT INTO `zipper`.`dog` (`name`)
VALUES ('汪-2');


-- test

USE `zipper`;

-- SELECT

SELECT *
FROM `zipper`.`cat`
WHERE `id` = 1;

-- INSERT

INSERT INTO `zipper`.`cat` (`name`)
VALUES ('喵-x');

-- UPDATE

UPDATE `zipper`.`cat`
SET `name` = '喵-0'
WHERE `id` = 1;

-- DELETE

DELETE
FROM `zipper`.`cat`
WHERE `id` = 1;

-- TRUNCATE

TRUNCATE TABLE `zipper`.`cat`;

-- ADD COLUMN

ALTER TABLE `zipper`.`cat`
    ADD COLUMN `age` INT NOT NULL DEFAULT 0 COMMENT '年龄';

-- MODIFY COLUMN

ALTER TABLE `zipper`.`cat`
    MODIFY COLUMN `age` INT NOT NULL DEFAULT 1 COMMENT '年龄';

-- CHANGE COLUMN

ALTER TABLE `zipper`.`cat`
    CHANGE COLUMN `age` `age` INT NOT NULL DEFAULT 2 COMMENT '年龄';

-- DROP COLUMN

ALTER TABLE `zipper`.`cat`
    DROP COLUMN `age`;

-- CREATE INDEX

CREATE INDEX `age` ON `zipper`.`cat` (`age`);

-- CREATE UNIQUE INDEX

CREATE UNIQUE INDEX `age` ON `zipper`.`cat` (`age`);

-- DROP INDEX

DROP INDEX `age` ON `zipper`.`cat`;


-- DROP TABLE

DROP TABLE `zipper`.`cat`;


-- DROP DATABASE

DROP DATABASE IF EXISTS `zipper`;

